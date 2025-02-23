package de.invesdwin.context.integration.channel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.AChannelTest.FileChannelType;
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
import de.invesdwin.context.log.error.Err;
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
public class ThroughputChannelTest {

    protected final AChannelTest parent;

    public ThroughputChannelTest(final AChannelTest parent) {
        this.parent = parent;
    }

    public void runQueueThroughputTest(final Queue<IReference<FDate>> channelQueue, final Object synchronizeChannel)
            throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = parent
                .maybeSynchronize(new QueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = parent
                .maybeSynchronize(new QueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(parent, channelReader);
        runThroughputTest(senderTask, receiverTask);
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    public void runBlockingQueueThroughputTest(final BlockingQueue<IReference<FDate>> channelQueue,
            final Object synchronizeChannel) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = parent
                .maybeSynchronize(new BlockingQueueSynchronousWriter<FDate>(channelQueue), synchronizeChannel);
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = parent
                .maybeSynchronize(new BlockingQueueSynchronousReader<FDate>(channelQueue), synchronizeChannel);
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(parent, channelReader);
        runThroughputTest(senderTask, receiverTask);
    }

    public void runThroughputTest(final FileChannelType pipes, final File channelFile, final Object synchronizeChannel)
            throws InterruptedException {
        runThroughputTest(pipes, channelFile, synchronizeChannel, DisabledChannelFactory.getInstance());
    }

    public void runThroughputTest(final FileChannelType pipes, final File channelFile, final Object synchronizeChannel,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapper)
            throws InterruptedException {
        runThroughputTest(pipes, channelFile, synchronizeChannel, wrapper, wrapper);
    }

    public void runThroughputTest(final FileChannelType pipes, final File channelFile, final Object synchronizeChannel,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperSender,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperReceiver)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> channelWriter = parent.maybeSynchronize(
                    wrapperSender.newWriter(parent.newWriter(channelFile, pipes)), synchronizeChannel);
            final ThroughputSenderTask senderTask = new ThroughputSenderTask(parent.newSerdeWriter(channelWriter));
            final ISynchronousReader<IByteBufferProvider> channelReader = parent.maybeSynchronize(
                    wrapperReceiver.newReader(parent.newReader(channelFile, pipes)), synchronizeChannel);
            final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(parent,
                    parent.newSerdeReader(channelReader));
            runThroughputTest(senderTask, receiverTask);
        } finally {
            Files.deleteQuietly(channelFile);
        }
    }

    public void runThroughputTest(final ThroughputSenderTask senderTask, final ThroughputReceiverTask receiverTask)
            throws InterruptedException {
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runThroughputTest", 1);
        try {
            final ListenableFuture<?> senderFuture = executor.submit(senderTask);
            receiverTask.run();
            Futures.get(senderFuture);
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination();
        }
    }

    public static class ThroughputReceiverTask implements Runnable {

        private final AChannelTest parent;
        private final OutputStream log;
        private final ISynchronousReader<FDate> channelReader;

        public ThroughputReceiverTask(final AChannelTest parent, final ISynchronousReader<FDate> channelReader) {
            this(parent, new Log(ThroughputReceiverTask.class), channelReader);
        }

        public ThroughputReceiverTask(final AChannelTest parent, final Log log,
                final ISynchronousReader<FDate> channelReader) {
            this(parent, Slf4jStream.of(log).asInfo(), channelReader);
        }

        public ThroughputReceiverTask(final AChannelTest parent, final OutputStream log,
                final ISynchronousReader<FDate> channelReader) {
            this.parent = parent;
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
                if (AChannelTest.DEBUG) {
                    log.write("receiver open channel reader\n".getBytes());
                }
                channelReader.open();
                try {
                    readsStart = new Instant();
                    final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(
                            channelReader);
                    while (count < AChannelTest.MESSAGE_COUNT) {
                        if (count == 0) {
                            //don't count in connection establishment
                            readsStart = new Instant();
                        }
                        final FDate readMessage = readSpinWait.waitForRead(AChannelTest.MAX_WAIT_DURATION);
                        channelReader.readFinished();
                        latencyReportMessageReceived.measureLatency(readMessage);
                        if (AChannelTest.DEBUG) {
                            log.write(("receiver channel in [" + readMessage + "]\n").getBytes());
                        }
                        Assertions.checkNotNull(readMessage);
                        latencyReportMessageReceived.validateOrder(prevValue, readMessage);
                        prevValue = readMessage;
                        count++;
                    }
                    Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
                    AChannelTest.printProgress(log, "ReadsFinished", readsStart, count, AChannelTest.MESSAGE_COUNT);
                    parent.assertCloseMessageArrived(channelReader);
                } finally {
                    if (AChannelTest.DEBUG) {
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
                if (AChannelTest.DEBUG) {
                    log.write("sender open channel writer\n".getBytes());
                }
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final LoopInterruptedCheck loopCheck = AChannelTest.newLoopInterruptedCheck();
                channelWriter.open();
                try {
                    Instant writesStart = new Instant();
                    try (ICloseableIterator<? extends IFDateProvider> values = latencyReportMessageSent
                            .newRequestMessages()
                            .iterator()) {
                        while (count < AChannelTest.MESSAGE_COUNT) {
                            if (count == 0) {
                                //don't count in connection establishment
                                writesStart = new Instant();
                            }
                            final IFDateProvider requestProvider = values.next();
                            final FDate value = requestProvider.asFDate();
                            writeSpinWait.waitForWrite(value, AChannelTest.MAX_WAIT_DURATION);
                            latencyReportMessageSent.measureLatency(value);
                            if (AChannelTest.DEBUG) {
                                log.write(("sender channel out [" + value + "]\n").getBytes());
                            }
                            if (loopCheck.checkNoInterrupt()) {
                                AChannelTest.printProgress(log, "Writes", writesStart, count,
                                        AChannelTest.MESSAGE_COUNT);
                            }
                            count++;
                        }
                    }
                    Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
                    AChannelTest.printProgress(log, "WritesFinished", writesStart, count, AChannelTest.MESSAGE_COUNT);
                } finally {
                    if (AChannelTest.DEBUG) {
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
