package de.invesdwin.context.integration.channel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.AChannelTest.FileChannelType;
import de.invesdwin.context.integration.channel.async.AsynchronousHandlerFactorySupport;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
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
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.IFDateProvider;

@NotThreadSafe
public class LatencyChannelTest {

    protected final AChannelTest parent;

    public LatencyChannelTest(final AChannelTest parent) {
        this.parent = parent;
    }

    public void runHandlerLatencyTest(final IAsynchronousChannel serverChannel,
            final IAsynchronousChannel clientChannel) throws InterruptedException {
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runHandlerLatencyTest", 1);
        try {
            final ListenableFuture<?> openFuture = executor.submit(() -> {
                try {
                    serverChannel.open();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
            clientChannel.open();
            while (!clientChannel.isClosed()) {
                FTimeUnit.MILLISECONDS.sleep(1);
            }
            //            while (!serverChannel.isClosed()) {
            //                FTimeUnit.MILLISECONDS.sleep(1);
            //            }
            openFuture.get(AChannelTest.MAX_WAIT_DURATION.longValue(),
                    AChannelTest.MAX_WAIT_DURATION.getTimeUnit().timeUnitValue());
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination();
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(clientChannel);
        }
    }

    public void runQueueLatencyTest(final Queue<IReference<FDate>> responseQueue,
            final Queue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = parent
                .maybeSynchronize(new QueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = parent
                .maybeSynchronize(new QueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final LatencyServerTask serverTask = new LatencyServerTask(parent, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = parent
                .maybeSynchronize(new QueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = parent
                .maybeSynchronize(new QueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        final LatencyClientTask clientTask = new LatencyClientTask(parent, requestWriter, responseReader);
        runLatencyTest(serverTask, clientTask);
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    public void runBlockingQueueLatencyTest(final BlockingQueue<IReference<FDate>> responseQueue,
            final BlockingQueue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = parent
                .maybeSynchronize(new BlockingQueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = parent
                .maybeSynchronize(new BlockingQueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final LatencyServerTask serverTask = new LatencyServerTask(parent, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = parent
                .maybeSynchronize(new BlockingQueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = parent
                .maybeSynchronize(new BlockingQueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        final LatencyClientTask clientTask = new LatencyClientTask(parent, requestWriter, responseReader);
        runLatencyTest(serverTask, clientTask);
    }

    public void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse) throws InterruptedException {
        runLatencyTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse,
                DisabledChannelFactory.getInstance());
    }

    public void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapper)
            throws InterruptedException {
        runLatencyTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse, wrapper, wrapper);
    }

    public void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperServer,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperClient)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = parent.maybeSynchronize(
                    wrapperServer.newWriter(parent.newWriter(responseFile, pipes)), synchronizeResponse);
            final ISynchronousReader<IByteBufferProvider> requestReader = parent.maybeSynchronize(
                    wrapperServer.newReader(parent.newReader(requestFile, pipes)), synchronizeRequest);
            final LatencyServerTask serverTask = new LatencyServerTask(parent, parent.newSerdeReader(requestReader),
                    parent.newSerdeWriter(responseWriter));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = parent.maybeSynchronize(
                    wrapperClient.newWriter(parent.newWriter(requestFile, pipes)), synchronizeRequest);
            final ISynchronousReader<IByteBufferProvider> responseReader = parent.maybeSynchronize(
                    wrapperClient.newReader(parent.newReader(responseFile, pipes)), synchronizeResponse);
            final LatencyClientTask clientTask = new LatencyClientTask(parent, parent.newSerdeWriter(requestWriter),
                    parent.newSerdeReader(responseReader));
            runLatencyTest(serverTask, clientTask);
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    public void runLatencyTest(final LatencyServerTask serverTask, final LatencyClientTask clientTask)
            throws InterruptedException {
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runLatencyTest", 1);
        try {
            final ListenableFuture<?> serverFuture = executor.submit(serverTask);
            clientTask.run();
            Futures.get(serverFuture);
        } catch (final Throwable t) {
            throw Err.process(t);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination();
        }
    }

    public static class LatencyClientTask implements Runnable {

        private final AChannelTest parent;
        private final OutputStream log;
        private final ISynchronousWriter<FDate> requestWriter;
        private final ISynchronousReader<FDate> responseReader;

        public LatencyClientTask(final AChannelTest parent, final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this(parent, new Log(LatencyClientTask.class), requestWriter, responseReader);
        }

        public LatencyClientTask(final AChannelTest parent, final Log log,
                final ISynchronousWriter<FDate> requestWriter, final ISynchronousReader<FDate> responseReader) {
            this(parent, Slf4jStream.of(log).asInfo(), requestWriter, responseReader);
        }

        public LatencyClientTask(final AChannelTest parent, final OutputStream log,
                final ISynchronousWriter<FDate> requestWriter, final ISynchronousReader<FDate> responseReader) {
            this.parent = parent;
            this.log = log;
            this.requestWriter = requestWriter;
            this.responseReader = responseReader;
        }

        @Override
        public void run() {
            try {
                Instant readsStart = new Instant();
                FDate prevValue = null;
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final ILatencyReportFactory latencyReportFactory = AChannelTest.LATENCY_REPORT_FACTORY;
                final ILatencyReport latencyReportRequestSent = latencyReportFactory
                        .newLatencyReport("latency/1_" + LatencyClientTask.class.getSimpleName() + "_requestSent");
                final ILatencyReport latencyReportResponseReceived = latencyReportFactory
                        .newLatencyReport("latency/4_" + LatencyClientTask.class.getSimpleName() + "_responseReceived");
                final ILatencyReport latencyReportRoundtrip = latencyReportFactory.newLatencyReport(
                        "latency/5_" + LatencyClientTask.class.getSimpleName() + "_requestResponseRoundtrip");
                if (AChannelTest.DEBUG) {
                    log.write("client open request writer\n".getBytes());
                }
                requestWriter.open();
                if (AChannelTest.DEBUG) {
                    log.write("client open response reader\n".getBytes());
                }
                responseReader.open();
                try {
                    final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(
                            responseReader);
                    final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(
                            requestWriter);
                    try (ICloseableIterator<? extends IFDateProvider> values = latencyReportRoundtrip
                            .newRequestMessages()
                            .iterator()) {
                        while (count < AChannelTest.MESSAGE_COUNT) {
                            if (count == 0) {
                                //don't count in connection establishment
                                readsStart = new Instant();
                            }
                            final IFDateProvider requestProvider = values.next();
                            final FDate request = requestProvider.asFDate();
                            writeSpinWait.waitForWrite(request, AChannelTest.MAX_WAIT_DURATION);
                            latencyReportRequestSent.measureLatency(request);
                            if (AChannelTest.DEBUG) {
                                log.write("client request out\n".getBytes());
                            }
                            final FDate response = readSpinWait.waitForRead(AChannelTest.MAX_WAIT_DURATION);
                            responseReader.readFinished();
                            final FDate arrivalTimestamp = latencyReportRoundtrip.newArrivalTimestamp().asFDate();
                            latencyReportResponseReceived.measureLatency(response, arrivalTimestamp);
                            latencyReportRoundtrip.measureLatency(request, arrivalTimestamp);
                            if (AChannelTest.DEBUG) {
                                log.write(("client response in [" + response + "]\n").getBytes());
                            }
                            latencyReportRoundtrip.validateResponse(request, response);
                            if (prevValue != null) {
                                Assertions.checkTrue(prevValue.isBefore(response));
                            }
                            prevValue = response;
                            count++;
                        }
                    } catch (final FastEOFException e) {
                        if (count != AChannelTest.MESSAGE_COUNT) {
                            throw e;
                        }
                    }
                    Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
                    AChannelTest.printProgress(log, "ReadsFinished", readsStart, count, AChannelTest.MESSAGE_COUNT);
                } finally {
                    if (AChannelTest.DEBUG) {
                        log.write("client close request writer\n".getBytes());
                    }
                    requestWriter.close();
                    parent.assertCloseMessageArrived(responseReader);
                    if (AChannelTest.DEBUG) {
                        log.write("client close response reader\n".getBytes());
                    }
                    responseReader.close();
                    latencyReportRequestSent.close();
                    latencyReportResponseReceived.close();
                    latencyReportRoundtrip.close();
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class LatencyServerTask implements Runnable {

        private final AChannelTest parent;
        private final OutputStream log;
        private final ISynchronousReader<FDate> requestReader;
        private final ISynchronousWriter<FDate> responseWriter;

        public LatencyServerTask(final AChannelTest parent, final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this(parent, new Log(LatencyServerTask.class), requestReader, responseWriter);
        }

        public LatencyServerTask(final AChannelTest parent, final Log log,
                final ISynchronousReader<FDate> requestReader, final ISynchronousWriter<FDate> responseWriter) {
            this(parent, Slf4jStream.of(log).asInfo(), requestReader, responseWriter);
        }

        public LatencyServerTask(final AChannelTest parent, final OutputStream log,
                final ISynchronousReader<FDate> requestReader, final ISynchronousWriter<FDate> responseWriter) {
            this.parent = parent;
            this.log = log;
            this.requestReader = requestReader;
            this.responseWriter = responseWriter;
        }

        @Override
        public void run() {
            try {
                final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(requestReader);
                final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(responseWriter);
                final ILatencyReport latencyReportRequestReceived = AChannelTest.LATENCY_REPORT_FACTORY
                        .newLatencyReport("latency/2_" + LatencyServerTask.class.getSimpleName() + "_requestReceived");
                final ILatencyReport latencyReportResponseSent = AChannelTest.LATENCY_REPORT_FACTORY
                        .newLatencyReport("latency/3_" + LatencyServerTask.class.getSimpleName() + "_responseSent");
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final LoopInterruptedCheck loopCheck = AChannelTest.newLoopInterruptedCheck();
                if (AChannelTest.DEBUG) {
                    log.write("server open request reader\n".getBytes());
                }
                requestReader.open();
                if (AChannelTest.DEBUG) {
                    log.write("server open response writer\n".getBytes());
                }
                responseWriter.open();
                Instant writesStart = new Instant();
                try {
                    while (count < AChannelTest.MESSAGE_COUNT) {
                        if (count == 0) {
                            //don't count in connection establishment
                            writesStart = new Instant();
                        }
                        final FDate request = readSpinWait.waitForRead(AChannelTest.MAX_WAIT_DURATION);
                        requestReader.readFinished();
                        latencyReportRequestReceived.measureLatency(request);
                        if (AChannelTest.DEBUG) {
                            log.write("server request in\n".getBytes());
                        }
                        final FDate response = latencyReportResponseSent.newResponseMessage(request).asFDate();
                        writeSpinWait.waitForWrite(response, AChannelTest.MAX_WAIT_DURATION);
                        latencyReportResponseSent.measureLatency(response);
                        if (AChannelTest.DEBUG) {
                            log.write(("server response out [" + response + "]\n").getBytes());
                        }
                        if (loopCheck.checkNoInterrupt()) {
                            AChannelTest.printProgress(log, "Writes", writesStart, count, AChannelTest.MESSAGE_COUNT);
                        }
                        count++;
                    }
                    Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
                    AChannelTest.printProgress(log, "WritesFinished", writesStart, count, AChannelTest.MESSAGE_COUNT);
                } finally {
                    if (AChannelTest.DEBUG) {
                        log.write("server close response writer\n".getBytes());
                    }
                    responseWriter.close();
                    parent.assertCloseMessageArrived(requestReader);
                    if (AChannelTest.DEBUG) {
                        log.write("server close request reader\n".getBytes());
                    }
                    requestReader.close();
                    latencyReportRequestReceived.close();
                    latencyReportResponseSent.close();
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class LatencyClientHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new LatencyClientHandler();
        }

    }

    public static class LatencyClientHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant readsStart;
        private int count;
        private ICloseableIterator<? extends IFDateProvider> values;
        private FDate prevValue;
        private ILatencyReport latencyReportResponseReceived;
        private ILatencyReport latencyReportRequestResponseRoundtrip;
        private FDate request;

        public LatencyClientHandler() {
            this(new Log(LatencyClientHandler.class));
        }

        public LatencyClientHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public LatencyClientHandler(final OutputStream log) {
            this.log = log;
        }

        @Override
        public FDate open(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            readsStart = new Instant();
            prevValue = null;
            count = -AChannelTest.WARMUP_MESSAGE_COUNT;

            final ILatencyReportFactory latencyReportFactory = AChannelTest.LATENCY_REPORT_FACTORY;
            latencyReportResponseReceived = latencyReportFactory.newLatencyReport(
                    "latencyHandler/2_" + LatencyClientHandler.class.getSimpleName() + "_responseReceived");
            latencyReportRequestResponseRoundtrip = latencyReportFactory.newLatencyReport(
                    "latencyHandler/3_" + LatencyClientHandler.class.getSimpleName() + "_requestResponseRoundtrip");
            this.values = latencyReportRequestResponseRoundtrip.newRequestMessages().iterator();
            request = values.next().asFDate();
            return request;
        }

        @Override
        public FDate idle(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            throw FastEOFException.getInstance("should not become idle");
        }

        @Override
        public FDate handle(final IAsynchronousHandlerContext<FDate> context, final FDate response) throws IOException {
            if (count == 0) {
                readsStart = new Instant();
            }
            final FDate arrivalTimestamp = latencyReportRequestResponseRoundtrip.newArrivalTimestamp().asFDate();
            latencyReportResponseReceived.measureLatency(response, arrivalTimestamp);
            latencyReportRequestResponseRoundtrip.measureLatency(request, arrivalTimestamp);
            if (AChannelTest.DEBUG) {
                log.write("client request out\n".getBytes());
            }
            if (AChannelTest.DEBUG) {
                log.write(("client response in [" + response + "]\n").getBytes());
            }
            Assertions.checkNotNull(response);
            latencyReportRequestResponseRoundtrip.validateResponse(request, response);
            latencyReportRequestResponseRoundtrip.validateOrder(prevValue, response);
            prevValue = response;
            count++;
            if (count > AChannelTest.MESSAGE_COUNT) {
                throw FastEOFException.getInstance("MESSAGE_COUNT exceeded");
            }
            request = values.next().asFDate();
            return request;
        }

        @Override
        public void outputFinished(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            //noop
        }

        @Override
        public void close() throws IOException {
            Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
            if (AChannelTest.DEBUG) {
                log.write("client close handler\n".getBytes());
            }
            AChannelTest.printProgress(log, "ReadsFinished", readsStart, count, AChannelTest.MESSAGE_COUNT);
            latencyReportResponseReceived.close();
            latencyReportResponseReceived = null;
            latencyReportRequestResponseRoundtrip.close();
            latencyReportRequestResponseRoundtrip = null;
        }

    }

    public static class LatencyServerHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new LatencyServerHandler();
        }

    }

    public static class LatencyServerHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant writesStart;
        private int count;
        private final LoopInterruptedCheck loopCheck = AChannelTest.newLoopInterruptedCheck();
        private ILatencyReport latencyReportRequestReceived;

        public LatencyServerHandler() {
            this(new Log(LatencyServerHandler.class));
        }

        public LatencyServerHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public LatencyServerHandler(final OutputStream log) {
            this.log = log;
        }

        @Override
        public FDate open(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            writesStart = new Instant();
            count = -AChannelTest.WARMUP_MESSAGE_COUNT;
            this.latencyReportRequestReceived = AChannelTest.LATENCY_REPORT_FACTORY.newLatencyReport(
                    "latencyHandler/1_" + LatencyServerHandler.class.getSimpleName() + "_requestReceived");

            if (AChannelTest.DEBUG) {
                log.write("server open handler\n".getBytes());
            }
            return null;
        }

        @Override
        public FDate idle(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            throw FastEOFException.getInstance("should not become idle");
        }

        @Override
        public FDate handle(final IAsynchronousHandlerContext<FDate> context, final FDate request) throws IOException {
            if (count == 0) {
                writesStart = new Instant();
            }
            if (AChannelTest.DEBUG) {
                log.write("server request in\n".getBytes());
            }
            try {
                final FDate response = latencyReportRequestReceived.newResponseMessage(request).asFDate();
                if (AChannelTest.DEBUG) {
                    log.write(("server response out [" + response + "]\n").getBytes());
                }
                count++;
                if (loopCheck.checkNoInterrupt()) {
                    AChannelTest.printProgress(log, "Writes", writesStart, count, AChannelTest.MESSAGE_COUNT);
                }
                if (count > AChannelTest.MESSAGE_COUNT) {
                    throw FastEOFException.getInstance("MESSAGE_COUNT exceeded");
                }
                return response;
            } catch (final NoSuchElementException e) {
                throw FastEOFException.getInstance(e);
            }
        }

        @Override
        public void outputFinished(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            //noop
        }

        @Override
        public void close() throws IOException {
            Assertions.checkEquals(AChannelTest.MESSAGE_COUNT, count);
            AChannelTest.printProgress(log, "WritesFinished", writesStart, count, AChannelTest.MESSAGE_COUNT);
            if (AChannelTest.DEBUG) {
                log.write("server close handler\n".getBytes());
            }
            latencyReportRequestReceived.close();
            latencyReportRequestReceived = null;
        }

    }

}
