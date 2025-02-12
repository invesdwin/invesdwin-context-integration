package de.invesdwin.context.integration.channel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.report.ILatencyReport;
import de.invesdwin.context.integration.channel.report.ILatencyReportFactory;
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
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.IFDateProvider;

@NotThreadSafe
public abstract class AThroughputChannelTest extends AChannelTest {

    protected void runQueueThroughputTest(final Queue<IReference<FDate>> channelQueue, final Object synchronizeChannel)
            throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(channelReader);
        runThroughputTest(senderTask, receiverTask);
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    protected void runBlockingQueueThroughputTest(final BlockingQueue<IReference<FDate>> channelQueue,
            final Object synchronizeChannel) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(channelReader);
        runThroughputTest(senderTask, receiverTask);
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
            final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(channelWriter));
            final ISynchronousReader<IByteBufferProvider> channelReader = maybeSynchronize(
                    wrapperReceiver.newReader(newReader(channelFile, pipes)), synchronizeChannel);
            final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(newSerdeReader(channelReader));
            runThroughputTest(senderTask, receiverTask);
        } finally {
            Files.deleteQuietly(channelFile);
        }
    }

    protected void runThroughputTest(final ThroughputSenderTask senderTask, final ThroughputReceiverTask receiverTask)
            throws InterruptedException {
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runThroughputTest", 1);
        try {
            final ListenableFuture<?> senderFuture = executor.submit(senderTask);
            receiverTask.run();
            Futures.get(senderFuture);
        } finally {
            executor.shutdown();
            executor.awaitTermination();
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
            try {
                Instant readsStart = new Instant();
                FDate prevValue = null;
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final ILatencyReportFactory latencyReportFactory = AChannelTest.LATENCY_REPORT_FACTORY;
                final ILatencyReport latencyReportMessageReceived = latencyReportFactory.newLatencyReport(
                        "throughput/2_" + ThroughputReceiverTask.class.getSimpleName() + "_messageReceived");
                if (DEBUG) {
                    log.write("receiver open channel reader\n".getBytes());
                }
                channelReader.open();
                try {
                    readsStart = new Instant();
                    final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(
                            channelReader);
                    while (count < MESSAGE_COUNT) {
                        if (count == 0) {
                            //don't count in connection establishment
                            readsStart = new Instant();
                        }
                        final FDate readMessage = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                        channelReader.readFinished();
                        latencyReportMessageReceived.measureLatency(readMessage);
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
                    Assertions.checkEquals(MESSAGE_COUNT, count);
                    printProgress(log, "ReadsFinished", readsStart, count, MESSAGE_COUNT);
                    assertCloseMessageArrived(channelReader);
                } finally {
                    if (DEBUG) {
                        log.write("receiver close channel reader\n".getBytes());
                    }
                    channelReader.close();
                    latencyReportMessageReceived.close();
                }
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
            try {
                final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(channelWriter);
                final ILatencyReportFactory latencyReportFactory = AChannelTest.LATENCY_REPORT_FACTORY;
                final ILatencyReport latencyReportMessageSent = latencyReportFactory.newLatencyReport(
                        "throughput/1_" + ThroughputSenderTask.class.getSimpleName() + "_messageSent");
                if (DEBUG) {
                    log.write("sender open channel writer\n".getBytes());
                }
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final LoopInterruptedCheck loopCheck = newLoopInterruptedCheck();
                channelWriter.open();
                try {
                    Instant writesStart = new Instant();
                    try (ICloseableIterator<? extends IFDateProvider> values = latencyReportMessageSent
                            .newRequestMessages()
                            .iterator()) {
                        while (count < MESSAGE_COUNT) {
                            if (count == 0) {
                                //don't count in connection establishment
                                writesStart = new Instant();
                            }
                            final IFDateProvider requestProvider = values.next();
                            final FDate value = requestProvider.asFDate();
                            writeSpinWait.waitForWrite(value, MAX_WAIT_DURATION);
                            latencyReportMessageSent.measureLatency(value);
                            if (DEBUG) {
                                log.write(("sender channel out [" + value + "]\n").getBytes());
                            }
                            if (loopCheck.checkNoInterrupt()) {
                                printProgress(log, "Writes", writesStart, count, MESSAGE_COUNT);
                            }
                            count++;
                        }
                    }
                    Assertions.checkEquals(MESSAGE_COUNT, count);
                    printProgress(log, "WritesFinished", writesStart, count, MESSAGE_COUNT);
                } finally {
                    if (DEBUG) {
                        log.write("sender close channel writer\n".getBytes());
                    }
                    channelWriter.close();
                    latencyReportMessageSent.close();
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
