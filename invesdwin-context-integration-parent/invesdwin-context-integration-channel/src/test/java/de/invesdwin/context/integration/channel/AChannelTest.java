package de.invesdwin.context.integration.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.async.AsynchronousHandlerFactorySupport;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.async.serde.SerdeAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient;
import de.invesdwin.context.integration.channel.rpc.client.session.multi.MultipleMultiplexingSynchronousEndpointClientSessionPool;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.DefaultSynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.service.IRpcTestService;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcClientTask;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestService;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.sync.DisabledChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannelFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mapped.blocking.BlockingMappedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.blocking.BlockingMappedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.unsafe.NativePipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AChannelTest extends ATest {

    public static final FDate REQUEST_MESSAGE = FDates.MAX_DATE;
    public static final boolean DEBUG = false;
    public static final int MAX_MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    public static final int VALUES = DEBUG ? 10 : 1_000;
    public static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    public static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);
    public static final int RPC_CLIENT_THREADS = DEBUG ? 1 : 10;
    public static final int RPC_CLIENT_TRANSPORTS = DEBUG ? 1 : 4;

    public enum FileChannelType {
        PIPE_STREAMING,
        PIPE_NATIVE,
        PIPE,
        MAPPED,
        BLOCKING_MAPPED,
        UNIX_SOCKET;
    }

    @SuppressWarnings("unchecked")
    protected void runRpcPerformanceTest(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory,
            final RpcTestServiceMode mode) throws InterruptedException {
        final SynchronousEndpointServer serverChannel = new SynchronousEndpointServer(serverAcceptor) {
            @Override
            protected int newMaxIoThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        serverChannel.register(IRpcTestService.class, new RpcTestService());
        final SynchronousEndpointClient<IRpcTestService> client = new SynchronousEndpointClient<>(
                new MultipleMultiplexingSynchronousEndpointClientSessionPool(
                        new DefaultSynchronousEndpointSessionFactory(clientEndpointFactory)) {
                    @Override
                    protected int newMaxSessionsCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                }, IRpcTestService.class);
        try {
            final WrappedExecutorService clientExecutor = Executors.newFixedThreadPool("runRpcPerformanceTest_client",
                    RPC_CLIENT_THREADS);
            serverChannel.open();
            try (IBufferingIterator<Future<?>> clientFutures = new BufferingIterator<>()) {
                for (int i = 0; i < RPC_CLIENT_THREADS; i++) {
                    clientFutures.add(clientExecutor.submit(new RpcClientTask(client, String.valueOf(i + 1), mode)));
                }
                while (clientFutures.hasNext()) {
                    Futures.getNoInterrupt(clientFutures.next());
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(client);
            Closeables.closeQuietly(serverChannel);
        }
    }

    protected void runHandlerPerformanceTest(final IAsynchronousChannel serverChannel,
            final IAsynchronousChannel clientChannel) throws InterruptedException {
        try {
            final WrappedExecutorService executor = Executors.newFixedThreadPool("runHandlerPerformanceTest", 1);
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
            while (!serverChannel.isClosed()) {
                FTimeUnit.MILLISECONDS.sleep(1);
            }
            openFuture.get(MAX_WAIT_DURATION.longValue(), MAX_WAIT_DURATION.getTimeUnit().timeUnitValue());
        } catch (ExecutionException | TimeoutException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(serverChannel);
            Closeables.closeQuietly(clientChannel);
        }
    }

    protected void runQueuePerformanceTest(final Queue<IReference<FDate>> responseQueue,
            final Queue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new ServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        new ClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    /**
     * WARNING: causes cpu spikes
     */
    @Deprecated
    protected void runBlockingQueuePerformanceTest(final BlockingQueue<IReference<FDate>> responseQueue,
            final BlockingQueue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBlockingQueuePerformance", 1);
        executor.execute(new ServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        new ClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected File newFile(final String name, final boolean tmpfs, final FileChannelType type) {
        final File baseFolder;
        if (tmpfs) {
            baseFolder = SynchronousChannels.getTmpfsFolderOrFallback();
        } else {
            baseFolder = ContextProperties.TEMP_DIRECTORY;
        }
        final File file = new File(baseFolder, name);
        Files.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (type == FileChannelType.UNIX_SOCKET) {
            return file;
        } else if (type == FileChannelType.PIPE || type == FileChannelType.PIPE_STREAMING
                || type == FileChannelType.PIPE_NATIVE) {
            if (SynchronousChannels.isNamedPipeSupported() && !OperatingSystem.isWindows()) {
                Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
            } else {
                try {
                    Files.touch(file);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (type == FileChannelType.MAPPED || type == FileChannelType.BLOCKING_MAPPED) {
            try {
                Files.touch(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, type);
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    protected <T> ISynchronousReader<T> maybeSynchronize(final ISynchronousReader<T> reader, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(reader, synchronize);
        } else {
            return reader;
        }
    }

    protected <T> ISynchronousWriter<T> maybeSynchronize(final ISynchronousWriter<T> writer, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(writer, synchronize);
        } else {
            return writer;
        }
    }

    protected void runPerformanceTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse) throws InterruptedException {
        runPerformanceTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse,
                DisabledChannelFactory.getInstance());
    }

    protected void runPerformanceTest(final FileChannelType pipes, final File requestFile, final File responseFile,
            final Object synchronizeRequest, final Object synchronizeResponse,
            final ISynchronousChannelFactory<IByteBufferProvider, IByteBufferProvider> wrapper)
            throws InterruptedException {
        runPerformanceTest(pipes, requestFile, responseFile, synchronizeRequest, synchronizeResponse, wrapper, wrapper);
    }

    protected void runPerformanceTest(final FileChannelType pipes, final File requestFile, final File responseFile,
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
            executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferProvider> requestWriter = maybeSynchronize(
                    wrapperClient.newWriter(newWriter(requestFile, pipes)), synchronizeRequest);
            final ISynchronousReader<IByteBufferProvider> responseReader = maybeSynchronize(
                    wrapperClient.newReader(newReader(responseFile, pipes)), synchronizeResponse);
            new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    protected ISynchronousReader<IByteBufferProvider> newReader(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_NATIVE) {
            return new NativePipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.BLOCKING_MAPPED) {
            return new BlockingMappedSynchronousReader(file, getMaxMessageSize());
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected int getMaxMessageSize() {
        return MAX_MESSAGE_SIZE;
    }

    protected ISynchronousWriter<IByteBufferProvider> newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_NATIVE) {
            return new NativePipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, getMaxMessageSize());
        } else if (pipes == FileChannelType.BLOCKING_MAPPED) {
            return new BlockingMappedSynchronousWriter(file, getMaxMessageSize());
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> newCommandHandlerFactory(
            final IAsynchronousHandlerFactory<FDate, FDate> handler) {
        return new SerdeAsynchronousHandlerFactory<FDate, FDate>(handler, FDateSerde.GET, FDateSerde.GET,
                FDateSerde.FIXED_LENGTH);
    }

    public static ISynchronousReader<FDate> newCommandReader(final ISynchronousReader<IByteBufferProvider> reader) {
        return new SerdeSynchronousReader<FDate>(reader, FDateSerde.GET);
    }

    public static ISynchronousWriter<FDate> newCommandWriter(final ISynchronousWriter<IByteBufferProvider> writer) {
        return new SerdeSynchronousWriter<FDate>(writer, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    public static void printProgress(final OutputStream log, final String action, final Instant start, final int count,
            final int maxCount) throws IOException {
        final Duration duration = start.toDuration();
        log.write(
                TextDescription
                        .format("%s: %s/%s (%s) %s during %s\n", action, count, maxCount,
                                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                                new ProcessedEventsRateString(count, duration), duration)
                        .getBytes());
    }

    public static ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDates.MIN_DATE, FDates.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    public static class ClientTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousWriter<FDate> requestWriter;
        private final ISynchronousReader<FDate> responseReader;

        public ClientTask(final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this(new Log(ClientTask.class), requestWriter, responseReader);
        }

        public ClientTask(final Log log, final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this(Slf4jStream.of(log).asInfo(), requestWriter, responseReader);
        }

        public ClientTask(final OutputStream log, final ISynchronousWriter<FDate> requestWriter,
                final ISynchronousReader<FDate> responseReader) {
            this.log = log;
            this.requestWriter = requestWriter;
            this.responseReader = responseReader;
        }

        @Override
        public void run() {
            Instant readsStart = new Instant();
            FDate prevValue = null;
            int count = 0;
            try {
                if (DEBUG) {
                    log.write("client open request writer\n".getBytes());
                }
                requestWriter.open();
                if (DEBUG) {
                    log.write("client open response reader\n".getBytes());
                }
                responseReader.open();
                readsStart = new Instant();
                final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(responseReader);
                final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(requestWriter);
                while (count < VALUES) {
                    writeSpinWait.waitForWrite(REQUEST_MESSAGE, MAX_WAIT_DURATION);
                    if (DEBUG) {
                        log.write("client request out\n".getBytes());
                    }
                    final FDate readMessage = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                    responseReader.readFinished();
                    if (DEBUG) {
                        log.write(("client response in [" + readMessage + "]\n").getBytes());
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
                    log.write("client close response reader\n".getBytes());
                }
                responseReader.close();
                if (DEBUG) {
                    log.write("client close request writer\n".getBytes());
                }
                requestWriter.close();
                printProgress(log, "ReadsFinished", readsStart, VALUES, VALUES);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class ServerTask implements Runnable {

        private final OutputStream log;
        private final ISynchronousReader<FDate> requestReader;
        private final ISynchronousWriter<FDate> responseWriter;

        public ServerTask(final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this(new Log(ServerTask.class), requestReader, responseWriter);
        }

        public ServerTask(final Log log, final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this(Slf4jStream.of(log).asInfo(), requestReader, responseWriter);
        }

        public ServerTask(final OutputStream log, final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this.log = log;
            this.requestReader = requestReader;
            this.responseWriter = responseWriter;
        }

        @Override
        public void run() {
            final SynchronousReaderSpinWait<FDate> readSpinWait = new SynchronousReaderSpinWait<>(requestReader);
            final SynchronousWriterSpinWait<FDate> writeSpinWait = new SynchronousWriterSpinWait<>(responseWriter);
            try {
                int i = 0;
                if (DEBUG) {
                    log.write("server open request reader\n".getBytes());
                }
                requestReader.open();
                if (DEBUG) {
                    log.write("server open response writer\n".getBytes());
                }
                responseWriter.open();
                final Instant writesStart = new Instant();
                for (final FDate date : newValues()) {
                    final FDate readMessage = readSpinWait.waitForRead(MAX_WAIT_DURATION);
                    if (DEBUG) {
                        log.write("server request in\n".getBytes());
                    }
                    requestReader.readFinished();
                    Assertions.checkEquals(readMessage, REQUEST_MESSAGE);
                    writeSpinWait.waitForWrite(date, MAX_WAIT_DURATION);
                    if (DEBUG) {
                        log.write(("server response out [" + date + "]\n").getBytes());
                    }
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        printProgress(log, "Writes", writesStart, i, VALUES);
                    }
                }
                printProgress(log, "WritesFinished", writesStart, VALUES, VALUES);
                if (DEBUG) {
                    log.write("server close response writer\n".getBytes());
                }
                responseWriter.close();
                if (DEBUG) {
                    log.write("server close request reader\n".getBytes());
                }
                requestReader.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class ReaderHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new ReaderHandler();
        }

    }

    public static class ReaderHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant readsStart;
        private int count;
        private FDate prevValue;

        public ReaderHandler() {
            this(new Log(ReaderHandler.class));
        }

        public ReaderHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public ReaderHandler(final OutputStream log) {
            this.log = log;
        }

        @Override
        public FDate open(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            readsStart = new Instant();
            prevValue = null;
            count = 0;
            return REQUEST_MESSAGE;
        }

        @Override
        public FDate idle(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            throw FastEOFException.getInstance("should not become idle");
        }

        @Override
        public FDate handle(final IAsynchronousHandlerContext<FDate> context, final FDate readMessage)
                throws IOException {
            if (DEBUG) {
                log.write("client request out\n".getBytes());
            }
            if (DEBUG) {
                log.write(("client response in [" + readMessage + "]\n").getBytes());
            }
            Assertions.checkNotNull(readMessage);
            if (prevValue != null && !prevValue.isBefore(readMessage)) {
                Assertions.assertThat(prevValue).isBefore(readMessage);
            }
            prevValue = readMessage;
            count++;
            return REQUEST_MESSAGE;
        }

        @Override
        public void outputFinished(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            //noop
        }

        @Override
        public void close() throws IOException {
            Assertions.checkEquals(count, VALUES);
            if (DEBUG) {
                log.write("client close handler\n".getBytes());
            }
            printProgress(log, "ReadsFinished", readsStart, VALUES, VALUES);
        }

    }

    public static class WriterHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new WriterHandler();
        }

    }

    public static class WriterHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant writesStart;
        private int i;
        private ICloseableIterator<FDate> values;

        public WriterHandler() {
            this(new Log(WriterHandler.class));
        }

        public WriterHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public WriterHandler(final OutputStream log) {
            this.log = log;
        }

        @Override
        public FDate open(final IAsynchronousHandlerContext<FDate> context) throws IOException {
            writesStart = new Instant();
            i = 0;
            this.values = newValues().iterator();
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
        public FDate handle(final IAsynchronousHandlerContext<FDate> context, final FDate readMessage)
                throws IOException {
            if (DEBUG) {
                log.write("server request in\n".getBytes());
            }
            Assertions.checkEquals(readMessage, REQUEST_MESSAGE);
            try {
                final FDate date = values.next();
                if (DEBUG) {
                    log.write(("server response out [" + date + "]\n").getBytes());
                }
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    printProgress(log, "Writes", writesStart, i, VALUES);
                }
                return date;
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
            printProgress(log, "WritesFinished", writesStart, VALUES, VALUES);
            if (DEBUG) {
                log.write("server close handler\n".getBytes());
            }
        }

    }

}
