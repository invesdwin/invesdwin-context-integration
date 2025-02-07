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
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;

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

    public static class LatencyReaderHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new LatencyReaderHandler();
        }

    }

    public static class LatencyReaderHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant readsStart;
        private int count;
        private FDate prevValue;

        public LatencyReaderHandler() {
            this(new Log(LatencyReaderHandler.class));
        }

        public LatencyReaderHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public LatencyReaderHandler(final OutputStream log) {
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

    public static class LatencyWriterHandlerFactory extends AsynchronousHandlerFactorySupport<FDate, FDate> {

        @Override
        public IAsynchronousHandler<FDate, FDate> newHandler() {
            return new LatencyWriterHandler();
        }

    }

    public static class LatencyWriterHandler implements IAsynchronousHandler<FDate, FDate> {

        private final OutputStream log;
        private Instant writesStart;
        private int i;
        private ICloseableIterator<FDate> values;

        public LatencyWriterHandler() {
            this(new Log(LatencyWriterHandler.class));
        }

        public LatencyWriterHandler(final Log log) {
            this(Slf4jStream.of(log).asInfo());
        }

        public LatencyWriterHandler(final OutputStream log) {
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
