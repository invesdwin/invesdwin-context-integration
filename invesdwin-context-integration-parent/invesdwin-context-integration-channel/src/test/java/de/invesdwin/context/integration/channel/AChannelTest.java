package de.invesdwin.context.integration.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.serde.SerdeAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.MappedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.PipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.pipe.streaming.StreamingPipeSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.QueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.integration.channel.sync.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousReader;
import de.invesdwin.context.integration.channel.sync.serde.SerdeSynchronousWriter;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
//@Ignore("manual test")
public abstract class AChannelTest extends ATest {

    public static final FDate REQUEST_MESSAGE = FDate.MAX_DATE;
    public static final boolean DEBUG = false;
    public static final int MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    public static final int VALUES = DEBUG ? 10 : 10_000_000;
    public static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    public static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);

    public enum FileChannelType {
        PIPE_STREAMING,
        PIPE,
        MAPPED,
        UNIX_SOCKET;
    }

    protected void runHandlerPerformanceTest(final IAsynchronousChannel serverChannel,
            final IAsynchronousChannel clientChannel) throws InterruptedException {
        try {
            new Thread() {
                @Override
                public void run() {
                    serverChannel.open();
                }
            }.start();
            clientChannel.open();
            while (!serverChannel.isClosed()) {
                FTimeUnit.MILLISECONDS.sleep(1);
            }
            while (!clientChannel.isClosed()) {
                FTimeUnit.MILLISECONDS.sleep(1);
            }
        } finally {
            serverChannel.close();
            clientChannel.close();
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
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new QueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new QueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    protected void runBlockingQueuePerformanceTest(final BlockingQueue<IReference<FDate>> responseQueue,
            final BlockingQueue<IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(responseQueue), synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBlockingQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<FDate>(requestQueue), synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
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
        } else if (type == FileChannelType.PIPE || type == FileChannelType.PIPE_STREAMING) {
            Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
        } else if (type == FileChannelType.MAPPED) {
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
        try {
            final ISynchronousWriter<IByteBufferWriter> responseWriter = maybeSynchronize(
                    newWriter(responseFile, pipes), synchronizeResponse);
            final ISynchronousReader<IByteBuffer> requestReader = maybeSynchronize(newReader(requestFile, pipes),
                    synchronizeRequest);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferWriter> requestWriter = maybeSynchronize(newWriter(requestFile, pipes),
                    synchronizeRequest);
            final ISynchronousReader<IByteBuffer> responseReader = maybeSynchronize(newReader(responseFile, pipes),
                    synchronizeResponse);
            read(newCommandWriter(requestWriter), newCommandReader(responseReader));
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    protected ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE_STREAMING) {
            return new StreamingPipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    protected IAsynchronousHandler<IByteBuffer, IByteBufferWriter> newCommandHandler(
            final IAsynchronousHandler<FDate, FDate> handler) {
        return new SerdeAsynchronousHandler<>(handler, FDateSerde.GET, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    protected ISynchronousReader<FDate> newCommandReader(final ISynchronousReader<IByteBuffer> reader) {
        return new SerdeSynchronousReader<FDate>(reader, FDateSerde.GET);
    }

    protected ISynchronousWriter<FDate> newCommandWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new SerdeSynchronousWriter<FDate>(writer, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    protected void read(final ISynchronousWriter<FDate> requestWriter, final ISynchronousReader<FDate> responseReader) {

        Instant readsStart = new Instant();
        FDate prevValue = null;
        int count = 0;
        try {
            if (DEBUG) {
                log.info("client open request writer");
            }
            requestWriter.open();
            if (DEBUG) {
                log.info("client open response reader");
            }
            responseReader.open();
            readsStart = new Instant();
            final ASpinWait spinWait = new ASpinWait() {
                @Override
                protected boolean isConditionFulfilled() throws Exception {
                    return responseReader.hasNext();
                }
            };
            long waitingSinceNanos = System.nanoTime();
            while (true) {
                requestWriter.write(REQUEST_MESSAGE);
                if (DEBUG) {
                    log.info("client request out");
                }
                Assertions.checkTrue(spinWait.awaitFulfill(waitingSinceNanos, MAX_WAIT_DURATION));
                final FDate readMessage = responseReader.readMessage();
                if (DEBUG) {
                    log.info("client response in [" + readMessage + "]");
                }
                Assertions.checkNotNull(readMessage);
                if (prevValue != null) {
                    Assertions.checkTrue(prevValue.isBefore(readMessage));
                }
                prevValue = readMessage;
                count++;
                waitingSinceNanos = System.nanoTime();
            }
        } catch (final EOFException e) {
            //writer closed
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.checkEquals(count, VALUES);
        try {
            if (DEBUG) {
                log.info("client close response reader");
            }
            responseReader.close();
            if (DEBUG) {
                log.info("client close request writer");
            }
            requestWriter.close();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        printProgress("ReadsFinished", readsStart, VALUES, VALUES);
    }

    protected void printProgress(final String action, final Instant start, final int count, final int maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    protected ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    public class WriterTask implements Runnable {

        private final ISynchronousReader<FDate> requestReader;
        private final ISynchronousWriter<FDate> responseWriter;

        public WriterTask(final ISynchronousReader<FDate> requestReader,
                final ISynchronousWriter<FDate> responseWriter) {
            this.requestReader = requestReader;
            this.responseWriter = responseWriter;
        }

        @Override
        public void run() {
            final ASpinWait spinWait = new ASpinWait() {
                @Override
                protected boolean isConditionFulfilled() throws Exception {
                    return requestReader.hasNext();
                }
            };
            try {
                int i = 0;
                if (DEBUG) {
                    log.info("server open request reader");
                }
                requestReader.open();
                if (DEBUG) {
                    log.info("server open response writer");
                }
                responseWriter.open();
                final Instant writesStart = new Instant();
                long waitingSinceNanos = System.nanoTime();
                for (final FDate date : newValues()) {
                    Assertions.checkTrue(spinWait.awaitFulfill(waitingSinceNanos, MAX_WAIT_DURATION));
                    if (DEBUG) {
                        log.info("server request in");
                    }
                    final FDate readMessage = requestReader.readMessage();
                    Assertions.checkEquals(readMessage, REQUEST_MESSAGE);
                    responseWriter.write(date);
                    if (DEBUG) {
                        log.info("server response out [" + date + "]");
                    }
                    i++;
                    if (i % FLUSH_INTERVAL == 0) {
                        printProgress("Writes", writesStart, i, VALUES);
                    }
                    waitingSinceNanos = System.nanoTime();
                }
                printProgress("WritesFinished", writesStart, VALUES, VALUES);
                if (DEBUG) {
                    log.info("server close response writer");
                }
                responseWriter.close();
                if (DEBUG) {
                    log.info("server close request reader");
                }
                requestReader.close();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public class ReaderHandler implements IAsynchronousHandler<FDate, FDate> {

        private Instant readsStart;
        private int count;
        private FDate prevValue;

        public ReaderHandler() {
        }

        @Override
        public FDate open() throws IOException {
            readsStart = new Instant();
            prevValue = null;
            count = 0;
            return REQUEST_MESSAGE;
        }

        @Override
        public FDate handle(final FDate readMessage) throws IOException {
            if (DEBUG) {
                log.info("client request out");
            }
            if (DEBUG) {
                log.info("client response in [" + readMessage + "]");
            }
            Assertions.checkNotNull(readMessage);
            if (prevValue != null) {
                Assertions.checkTrue(prevValue.isBefore(readMessage));
            }
            prevValue = readMessage;
            count++;
            return REQUEST_MESSAGE;
        }

        @Override
        public void close() {
            Assertions.checkEquals(count, VALUES);
            if (DEBUG) {
                log.info("client close handler");
            }
            printProgress("ReadsFinished", readsStart, VALUES, VALUES);
        }

    }

    public class WriterHandler implements IAsynchronousHandler<FDate, FDate> {

        private Instant writesStart;
        private int i;
        private ICloseableIterator<FDate> values;

        public WriterHandler() {
        }

        @Override
        public FDate open() throws IOException {
            writesStart = new Instant();
            i = 0;
            this.values = newValues().iterator();
            if (DEBUG) {
                log.info("server open handler");
            }
            return null;
        }

        @Override
        public FDate handle(final FDate readMessage) throws IOException {
            if (DEBUG) {
                log.info("server request in");
            }
            Assertions.checkEquals(readMessage, REQUEST_MESSAGE);
            try {
                final FDate date = values.next();
                if (DEBUG) {
                    log.info("server response out [" + date + "]");
                }
                i++;
                if (i % FLUSH_INTERVAL == 0) {
                    printProgress("Writes", writesStart, i, VALUES);
                }
                return date;
            } catch (final NoSuchElementException e) {
                throw new EOFException("end reached");
            }
        }

        @Override
        public void close() {
            printProgress("WritesFinished", writesStart, VALUES, VALUES);
            if (DEBUG) {
                log.info("server close handler");
            }
        }

    }

}
