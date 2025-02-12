package de.invesdwin.context.integration.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import com.google.common.util.concurrent.ListenableFuture;

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
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
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
public abstract class ALatencyChannelTest extends AChannelTest {

    protected void runHandlerLatencyTest(final IAsynchronousChannel serverChannel,
            final IAsynchronousChannel clientChannel) throws InterruptedException {
        try {
            final WrappedExecutorService executor = Executors.newFixedThreadPool("runHandlerLatencyTest", 1);
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
            openFuture.get(MAX_WAIT_DURATION.longValue(), MAX_WAIT_DURATION.getTimeUnit().timeUnitValue());
        } catch (ExecutionException | TimeoutException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(clientChannel);
        }
    }

    protected void runQueueLatencyTest(final Queue<IReference<FDate>> responseQueue,
            final Queue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runQueueLatencyTest", 1);
        executor.execute(new LatencyServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        new LatencyClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    protected void runBlockingQueueLatencyTest(final BlockingQueue<IReference<FDate>> responseQueue,
            final BlockingQueue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runBlockingQueueLatencyTest", 1);
        executor.execute(new LatencyServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        new LatencyClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse) throws InterruptedException {
        runLatencyTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse,
                DisabledChannelFactory.getInstance());
    }

    protected void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapper)
            throws InterruptedException {
        runLatencyTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse, wrapper, wrapper);
    }

    protected void runLatencyTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperServer,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapperClient)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferProvider> responseWriter = maybeSynchronize(
                    wrapperServer.newWriter(newWriter(responseFile, pipes)), synchronizeResponse);
            final ISynchronousReader<IByteBufferProvider> requestReader = maybeSynchronize(
                    wrapperServer.newReader(newReader(requestFile, pipes)), synchronizeRequest);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new LatencyServerTask(newSerdeReader(requestReader), newSerdeWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = maybeSynchronize(
                    wrapperClient.newWriter(newWriter(requestFile, pipes)), synchronizeRequest);
            final ISynchronousReader<IByteBufferProvider> responseReader = maybeSynchronize(
                    wrapperClient.newReader(newReader(responseFile, pipes)), synchronizeResponse);
            new LatencyClientTask(newSerdeWriter(requestWriter), newSerdeReader(responseReader)).run();
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    public static class LatencyClientTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousWriter<FDate> requestWriter;
        private final ISynchronousReader<FDate> responseReader;

        public LatencyClientTask(final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this(new Log(LatencyClientTask.class), requestWriter, responseReader);
        }

        public LatencyClientTask(final Log log, final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this(Slf4jStream.of(log).asInfo(), requestWriter, responseReader);
        }

        public LatencyClientTask(final OutputStream log, final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this.log = log;
            this.requestWriter = requestWriter;
            this.responseReader = responseReader;
        }

        @Override
        public void run() {
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
            try {
                if (DEBUG) {
                    log.write("client open request writer\n".getBytes());
                }
                requestWriter.open();
                if (DEBUG) {
                    log.write("client open response reader\n".getBytes());
                }
                responseReader.open();
                final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(responseReader);
                final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(requestWriter);
                try (ICloseableIterator<? extends IFDateProvider> values = latencyReportRoundtrip.newRequestMessages()
                        .iterator()) {
                    while (count < ALatencyChannelTest.MESSAGE_COUNT) {
                        if (count == 0) {
                            //don't count in connection establishment
                            readsStart = new Instant();
                        }
                        final IFDateProvider requestProvider = values.next();
                        final FDate request = requestProvider.asFDate();
                        writeSpinWait.waitForWrite(request, MAX_WAIT_DURATION);
                        latencyReportRequestSent.measureLatency(request);
                        if (DEBUG) {
                            log.write("client request out\n".getBytes());
                        }
                        final FDate response = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                        responseReader.readFinished();
                        final FDate arrivalTimestamp = latencyReportRoundtrip.newArrivalTimestamp().asFDate();
                        latencyReportResponseReceived.measureLatency(response, arrivalTimestamp);
                        latencyReportRoundtrip.measureLatency(request, arrivalTimestamp);
                        if (DEBUG) {
                            log.write(("client response in [" + response + "]\n").getBytes());
                        }
                        latencyReportRoundtrip.validateResponse(request, response);
                        if (prevValue != null) {
                            Assertions.checkTrue(prevValue.isBefore(response));
                        }
                        prevValue = response;
                        count++;
                    }
                }
            } catch (final EOFException e) {
                //writer closed
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            try {
                Assertions.checkEquals(MESSAGE_COUNT, count);
                printProgress(log, "ReadsFinished", readsStart, count, MESSAGE_COUNT);
                if (DEBUG) {
                    log.write("client close response reader\n".getBytes());
                }
                responseReader.close();
                if (DEBUG) {
                    log.write("client close request writer\n".getBytes());
                }
                requestWriter.close();
                latencyReportRequestSent.close();
                latencyReportResponseReceived.close();
                latencyReportRoundtrip.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class LatencyServerTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousReader<FDate> requestReader;
        private final ISynchronousWriter<FDate> responseWriter;

        public LatencyServerTask(final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this(new Log(LatencyServerTask.class), requestReader, responseWriter);
        }

        public LatencyServerTask(final Log log, final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this(Slf4jStream.of(log).asInfo(), requestReader, responseWriter);
        }

        public LatencyServerTask(final OutputStream log, final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this.log = log;
            this.requestReader = requestReader;
            this.responseWriter = responseWriter;
        }

        @Override
        public void run() {
            final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(requestReader);
            final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(responseWriter);
            final ILatencyReport latencyReportRequestReceived = AChannelTest.LATENCY_REPORT_FACTORY
                    .newLatencyReport("latency/2_" + LatencyServerTask.class.getSimpleName() + "_requestReceived");
            final ILatencyReport latencyReportResponseSent = AChannelTest.LATENCY_REPORT_FACTORY
                    .newLatencyReport("latency/3_" + LatencyServerTask.class.getSimpleName() + "_responseSent");
            try {
                int count = -AChannelTest.WARMUP_MESSAGE_COUNT;
                final LoopInterruptedCheck loopCheck = newLoopInterruptedCheck();
                if (DEBUG) {
                    log.write("server open request reader\n".getBytes());
                }
                requestReader.open();
                if (DEBUG) {
                    log.write("server open response writer\n".getBytes());
                }
                responseWriter.open();
                Instant writesStart = new Instant();
                while (count < MESSAGE_COUNT) {
                    if (count == 0) {
                        //don't count in connection establishment
                        writesStart = new Instant();
                    }
                    final FDate request = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                    requestReader.readFinished();
                    latencyReportRequestReceived.measureLatency(request);
                    if (DEBUG) {
                        log.write("server request in\n".getBytes());
                    }
                    final FDate response = latencyReportResponseSent.newResponseMessage(request).asFDate();
                    writeSpinWait.waitForWrite(response, MAX_WAIT_DURATION);
                    latencyReportResponseSent.measureLatency(response);
                    if (DEBUG) {
                        log.write(("server response out [" + response + "]\n").getBytes());
                    }
                    if (loopCheck.checkNoInterrupt()) {
                        printProgress(log, "Writes", writesStart, count, MESSAGE_COUNT);
                    }
                    count++;
                }
                Assertions.checkEquals(MESSAGE_COUNT, count);
                printProgress(log, "WritesFinished", writesStart, count, MESSAGE_COUNT);
                if (DEBUG) {
                    log.write("server close response writer\n".getBytes());
                }
                responseWriter.close();
                if (DEBUG) {
                    log.write("server close request reader\n".getBytes());
                }
                requestReader.close();
                latencyReportRequestReceived.close();
                latencyReportResponseSent.close();
            } catch (final Exception e) {
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
            if (DEBUG) {
                log.write("client request out\n".getBytes());
            }
            if (DEBUG) {
                log.write(("client response in [" + response + "]\n").getBytes());
            }
            Assertions.checkNotNull(response);
            latencyReportRequestResponseRoundtrip.validateResponse(request, response);
            if (prevValue != null && !prevValue.isBefore(response)) {
                Assertions.assertThat(prevValue).isBefore(response);
            }
            prevValue = response;
            count++;
            if (count > MESSAGE_COUNT) {
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
            Assertions.checkEquals(MESSAGE_COUNT, count);
            if (DEBUG) {
                log.write("client close handler\n".getBytes());
            }
            printProgress(log, "ReadsFinished", readsStart, count, MESSAGE_COUNT);
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
        private final LoopInterruptedCheck loopCheck = newLoopInterruptedCheck();
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

            if (DEBUG) {
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
            if (DEBUG) {
                log.write("server request in\n".getBytes());
            }
            try {
                final FDate response = latencyReportRequestReceived.newResponseMessage(request).asFDate();
                if (DEBUG) {
                    log.write(("server response out [" + response + "]\n").getBytes());
                }
                count++;
                if (loopCheck.checkNoInterrupt()) {
                    printProgress(log, "Writes", writesStart, count, MESSAGE_COUNT);
                }
                if (count > MESSAGE_COUNT) {
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
            Assertions.checkEquals(MESSAGE_COUNT, count);
            printProgress(log, "WritesFinished", writesStart, count, MESSAGE_COUNT);
            if (DEBUG) {
                log.write("server close handler\n".getBytes());
            }
            latencyReportRequestReceived.close();
            latencyReportRequestReceived = null;
        }

    }

}
