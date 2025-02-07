package de.invesdwin.context.integration.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.integration.channel.sync.DisabledChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public abstract class AThroughputChannelTest extends AChannelTest {

    protected void runQueueThroughputTest(final Queue<IReference<FDate>> channelQueue, final Object synchronizeChannel)
            throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runQueueThroughputTest", 1);
        executor.execute(new ThroughputSenderTask(channelWriter));
        final ISynchronousReader<FDate> channelReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        new ThroughputReceiverTask(channelReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    protected void runBlockingQueueThroughputTest(final BlockingQueue<IReference<FDate>> channelQueue,
            final Object synchronizeChannel) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runBlockingQueueThroughputTest", 1);
        executor.execute(new ThroughputSenderTask(channelWriter));
        final ISynchronousReader<FDate> channelReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        new ThroughputReceiverTask(channelReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected void runThroughputTest(final FileChannelType pipes, final File channelFile,
            final Object synchronizeChannel) throws InterruptedException {
        runThroughputTest(pipes, channelFile, synchronizeChannel, DisabledChannelFactory.getInstance());
    }

    protected void runThroughputTest(final FileChannelType pipes, final File channelFile,
            final Object synchronizeChannel,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapper)
            throws InterruptedException {
        runThroughputTest(pipes, channelFile, synchronizeChannel, wrapper, wrapper);
    }

    protected void runThroughputTest(final FileChannelType pipes, final File channelFile,
            final Object synchronizeChannel,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperSender,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperReceiver)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> channelWriter = maybeSynchronize(
                    wrapperSender.newWriter(newWriter(channelFile, pipes)), synchronizeChannel);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(channelFile.getName(), 1);
            executor.execute(new ThroughputSenderTask(newSerdeWriter(channelWriter)));
            final ISynchronousReader<IByteBufferProvider> channelReader = maybeSynchronize(
                    wrapperReceiver.newReader(newReader(channelFile, pipes)), synchronizeChannel);
            new ThroughputReceiverTask(newSerdeReader(channelReader)).run();
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(channelFile);
        }
    }

    public static class ThroughputReceiverTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousReader<FDate> channelReader;

        public ThroughputReceiverTask(final ISynchronousReader<FDate> channelReader) {
            this(new Log(ThroughputReceiverTask.class), channelReader);
        }

        public ThroughputReceiverTask(final Log log, final ISynchronousReader<FDate> channelReader) {
            this(Slf4jStream.of(log).asInfo(), channelReader);
        }

        public ThroughputReceiverTask(final OutputStream log, final ISynchronousReader<FDate> channelReader) {
            this.log = log;
            this.channelReader = channelReader;
        }

        @Override
        public void run() {
            Instant readsStart = new Instant();
            FDate prevValue = null;
            int count = 0;
            try {
                if (DEBUG) {
                    log.write("receiver open channel reader\n".getBytes());
                }
                channelReader.open();
                readsStart = new Instant();
                final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(channelReader);
                while (count < VALUES) {
                    final FDate readMessage = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                    channelReader.readFinished();
                    if (DEBUG) {
                        log.write(("receiver channel in [" + readMessage + "]\n").getBytes());
                    }
                    Assertions.checkNotNull(readMessage);
                    if (prevValue != null) {
                        Assertions.checkTrue(prevValue.isBefore(readMessage));
                    }
                    prevValue = readMessage;
                    count++;
                }
            } catch (final EOFException e) {
                //writer closed
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            Assertions.checkEquals(VALUES, count);
            try {
                if (DEBUG) {
                    log.write("receiver close channel reader\n".getBytes());
                }
                channelReader.close();
                printProgress(log, "ReadsFinished", readsStart, VALUES, VALUES);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class ThroughputSenderTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousWriter<FDate> channelWriter;

        public ThroughputSenderTask(final ISynchronousWriter<FDate> channelWriter) {
            this(new Log(ThroughputSenderTask.class), channelWriter);
        }

        public ThroughputSenderTask(final Log log, final ISynchronousWriter<FDate> channelWriter) {
            this(Slf4jStream.of(log).asInfo(), channelWriter);
        }

        public ThroughputSenderTask(final OutputStream log, final ISynchronousWriter<FDate> channelWriter) {
            this.log = log;
            this.channelWriter = channelWriter;
        }

        @Override
        public void run() {
            final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(channelWriter);
            try {
                int i = 0;
                if (DEBUG) {
                    log.write("sender open channel writer\n".getBytes());
                }
                channelWriter.open();
                final Instant writesStart = new Instant();
                for (final FDate date : newValues()) {
                    writeSpinWait.waitForWrite(date, MAX_WAIT_DURATION);
                    if (DEBUG) {
                        log.write(("sender channel out [" + date + "]\n").getBytes());
                    }
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        printProgress(log, "Writes", writesStart, i, VALUES);
                    }
                }
                printProgress(log, "WritesFinished", writesStart, VALUES, VALUES);
                if (DEBUG) {
                    log.write("sender close channel writer\n".getBytes());
                }
                channelWriter.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
