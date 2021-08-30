package de.invesdwin.context.persistence.channel;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscLinkedQueue;
import org.jctools.queues.atomic.SpscAtomicArrayQueue;
import org.jctools.queues.atomic.SpscLinkedAtomicQueue;
import org.junit.Test;

import com.conversantmedia.util.concurrent.ConcurrentQueue;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.MultithreadConcurrentQueue;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
import com.conversantmedia.util.concurrent.PushPullConcurrentQueue;
import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.SynchronousChannels;
import de.invesdwin.context.integration.channel.aeron.AeronSynchronousReader;
import de.invesdwin.context.integration.channel.aeron.AeronSynchronousWriter;
import de.invesdwin.context.integration.channel.chronicle.ChronicleSynchronousReader;
import de.invesdwin.context.integration.channel.chronicle.ChronicleSynchronousWriter;
import de.invesdwin.context.integration.channel.conversant.ConversantSynchronousReader;
import de.invesdwin.context.integration.channel.conversant.ConversantSynchronousWriter;
import de.invesdwin.context.integration.channel.kryonet.KryonetSynchronousReader;
import de.invesdwin.context.integration.channel.kryonet.KryonetSynchronousWriter;
import de.invesdwin.context.integration.channel.lmax.LmaxSynchronousReader;
import de.invesdwin.context.integration.channel.lmax.LmaxSynchronousWriter;
import de.invesdwin.context.integration.channel.lmax.LmaxDisruptorQueue;
import de.invesdwin.context.integration.channel.mapped.MappedSynchronousReader;
import de.invesdwin.context.integration.channel.mapped.MappedSynchronousWriter;
import de.invesdwin.context.integration.channel.pipe.PipeSynchronousReader;
import de.invesdwin.context.integration.channel.pipe.PipeSynchronousWriter;
import de.invesdwin.context.integration.channel.pipe.streaming.StreamingPipeSynchronousReader;
import de.invesdwin.context.integration.channel.pipe.streaming.StreamingPipeSynchronousWriter;
import de.invesdwin.context.integration.channel.queue.QueueSynchronousReader;
import de.invesdwin.context.integration.channel.queue.QueueSynchronousWriter;
import de.invesdwin.context.integration.channel.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.integration.channel.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.integration.channel.reference.ReferenceSynchronousReader;
import de.invesdwin.context.integration.channel.reference.ReferenceSynchronousWriter;
import de.invesdwin.context.integration.channel.serde.SerdeSynchronousReader;
import de.invesdwin.context.integration.channel.serde.SerdeSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.tcp.SocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.tcp.SocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.tcp.blocking.BlockingSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.tcp.blocking.BlockingSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.blocking.BlockingDatagramSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.udp.blocking.BlockingDatagramSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.JeromqSynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.JeromqSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.type.IJeromqSocketType;
import de.invesdwin.context.integration.channel.zeromq.type.JeromqSocketType;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.reference.AtomicReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.JavaLockedReference;
import de.invesdwin.util.concurrent.reference.LockedReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.concurrent.reference.SynchronizedReference;
import de.invesdwin.util.concurrent.reference.VolatileReference;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.lang.uri.Addresses;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDates;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

// CHECKSTYLE:OFF
@NotThreadSafe
//@Ignore("manual test")
public class ChannelPerformanceTest extends ATest {
    //CHECKSTYLE:ON

    private static final FDate REQUEST_MESSAGE = FDate.MAX_DATE;
    private static final boolean DEBUG = false;
    private static final int MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    private static final int VALUES = DEBUG ? 10 : 100_000_000;
    private static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    private static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);

    private enum FileChannelType {
        PIPE_STREAMING,
        PIPE,
        MAPPED;
    }

    private File newFile(final String name, final boolean tmpfs, final FileChannelType pipes) {
        final File baseFolder;
        if (tmpfs) {
            baseFolder = SynchronousChannels.getTmpfsFolderOrFallback();
        } else {
            baseFolder = ContextProperties.TEMP_DIRECTORY;
        }
        final File file = new File(baseFolder, name);
        Files.deleteQuietly(file);
        Assertions.checkFalse(file.exists(), "%s", file);
        if (pipes == FileChannelType.PIPE || pipes == FileChannelType.PIPE_STREAMING) {
            Assertions.checkTrue(SynchronousChannels.createNamedPipe(file));
        } else if (pipes == FileChannelType.MAPPED) {
            try {
                Files.touch(file);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
        Assertions.checkTrue(file.exists());
        return file;
    }

    @Test
    public void testNamedPipePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipeStreamingPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.PIPE_STREAMING;
        final File requestFile = newFile("testNamedPipePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testNamedPipeStreamingPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.PIPE_STREAMING;
        final File requestFile = newFile("testNamedPipePerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testNamedPipePerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformance_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testMappedMemoryPerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testMappedMemoryPerformanceWithTmpfs_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testMappedMemoryPerformanceWithTmpfs_response.pipe", tmpfs, pipes);
        runPerformanceTest(pipes, requestFile, responseFile, null, null);
    }

    @Test
    public void testChroniclePerformance() throws InterruptedException {
        final boolean tmpfs = false;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformance_request" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformance_response" + SingleChronicleQueue.SUFFIX, tmpfs,
                pipes);
        Files.deleteQuietly(responseFile);
        runChroniclePerformanceTest(requestFile, responseFile);
    }

    @Test
    public void testChroniclePerformanceWithTmpfs() throws InterruptedException {
        final boolean tmpfs = true;
        final FileChannelType pipes = FileChannelType.MAPPED;
        final File requestFile = newFile("testChroniclePerformanceWithTmpfs_request" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(requestFile);
        final File responseFile = newFile("testChroniclePerformanceWithTmpfs_response" + SingleChronicleQueue.SUFFIX,
                tmpfs, pipes);
        Files.deleteQuietly(responseFile);
        runChroniclePerformanceTest(requestFile, responseFile);
    }

    private void runChroniclePerformanceTest(final File requestFile, final File responseFile)
            throws InterruptedException {
        try {
            final ISynchronousWriter<IByteBufferWriter> responseWriter = new ChronicleSynchronousWriter(responseFile);
            final ISynchronousReader<IByteBuffer> requestReader = new ChronicleSynchronousReader(requestFile);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
            final ISynchronousWriter<IByteBufferWriter> requestWriter = new ChronicleSynchronousWriter(requestFile);
            final ISynchronousReader<IByteBuffer> responseReader = new ChronicleSynchronousReader(responseFile);
            read(newCommandWriter(requestWriter), newCommandReader(responseReader));
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    @Test
    public void testArrayDequePerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final Queue<IReference<FDate>> responseQueue = new ArrayDeque<IReference<FDate>>(1);
        final Queue<IReference<FDate>> requestQueue = new ArrayDeque<IReference<FDate>>(1);
        runQueuePerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new LinkedBlockingQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new LinkedBlockingQueue<IReference<FDate>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedBlockingQueue<IReference<FDate>>(1);
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedBlockingQueue<IReference<FDate>>(1);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(1, false);
        final BlockingQueue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(1, false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlockingFair() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(1, true);
        final BlockingQueue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(1, true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformance() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedTransferQueue<IReference<FDate>>();
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedTransferQueue<IReference<FDate>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedTransferQueue<IReference<FDate>>();
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedTransferQueue<IReference<FDate>>();
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test(expected = AssertionError.class)
    public void testSynchronousQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(false);
        final Queue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(false);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlocking() throws InterruptedException {
        final SynchronousQueue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(false);
        final SynchronousQueue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlockingFair() throws InterruptedException {
        final SynchronousQueue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(true);
        final SynchronousQueue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaOneToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new OneToOneConcurrentArrayQueue<IReference<FDate>>(256);
        final Queue<IReference<FDate>> requestQueue = new OneToOneConcurrentArrayQueue<IReference<FDate>>(256);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new ManyToOneConcurrentArrayQueue<IReference<FDate>>(256);
        final Queue<IReference<FDate>> requestQueue = new ManyToOneConcurrentArrayQueue<IReference<FDate>>(256);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToManyConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new ManyToManyConcurrentArrayQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new ManyToManyConcurrentArrayQueue<IReference<FDate>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedAtomicQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscLinkedAtomicQueue<IReference<FDate>>();
        final Queue<IReference<FDate>> requestQueue = new SpscLinkedAtomicQueue<IReference<FDate>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscLinkedQueue<IReference<FDate>>();
        final Queue<IReference<FDate>> requestQueue = new SpscLinkedQueue<IReference<FDate>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscAtomicArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscAtomicArrayQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new SpscAtomicArrayQueue<IReference<FDate>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscArrayQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new SpscArrayQueue<IReference<FDate>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    private void runQueuePerformanceTest(final Queue<IReference<FDate>> responseQueue,
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

    private void runBlockingQueuePerformanceTest(final BlockingQueue<IReference<FDate>> responseQueue,
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

    @Test
    public void testSynchronizedReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new SynchronizedReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new SynchronizedReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testLockedReferencePerformance() throws InterruptedException {
        final ILock lock = ILockCollectionFactory.getInstance(true).newLock("asdf");
        final IMutableReference<IReference<FDate>> responseQueue = new LockedReference<IReference<FDate>>(lock);
        final IMutableReference<IReference<FDate>> requestQueue = new LockedReference<IReference<FDate>>(lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testJavaLockedReferencePerformance() throws InterruptedException {
        //CHECKSTYLE:OFF
        final Lock lock = new ReentrantLock();
        //CHECKSTYLE:ON
        final IMutableReference<IReference<FDate>> responseQueue = new JavaLockedReference<IReference<FDate>>(lock);
        final IMutableReference<IReference<FDate>> requestQueue = new JavaLockedReference<IReference<FDate>>(lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testAtomicReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new AtomicReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new AtomicReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testVolatileReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new VolatileReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new VolatileReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    private void runReferencePerformanceTest(final IMutableReference<IReference<FDate>> responseQueue,
            final IMutableReference<IReference<FDate>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = new ReferenceSynchronousWriter<FDate>(responseQueue);
        final ISynchronousReader<FDate> requestReader = new ReferenceSynchronousReader<FDate>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = new ReferenceSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new ReferenceSynchronousReader<FDate>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testConversantPushPullConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<IReference<FDate>> responseQueue = new PushPullConcurrentQueue<IReference<FDate>>(1);
        final ConcurrentQueue<IReference<FDate>> requestQueue = new PushPullConcurrentQueue<IReference<FDate>>(1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantPushPullBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<IReference<FDate>> responseQueue = new PushPullBlockingQueue<IReference<FDate>>(1);
        final ConcurrentQueue<IReference<FDate>> requestQueue = new PushPullBlockingQueue<IReference<FDate>>(1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<IReference<FDate>> responseQueue = new MultithreadConcurrentQueue<IReference<FDate>>(256);
        final ConcurrentQueue<IReference<FDate>> requestQueue = new MultithreadConcurrentQueue<IReference<FDate>>(256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<IReference<FDate>> responseQueue = new DisruptorBlockingQueue<IReference<FDate>>(256);
        final ConcurrentQueue<IReference<FDate>> requestQueue = new DisruptorBlockingQueue<IReference<FDate>>(256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    private void runConversantPerformanceTest(final ConcurrentQueue<IReference<FDate>> responseQueue,
            final ConcurrentQueue<IReference<FDate>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = new ConversantSynchronousWriter<FDate>(responseQueue);
        final ISynchronousReader<FDate> requestReader = new ConversantSynchronousReader<FDate>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = new ConversantSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new ConversantSynchronousReader<FDate>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testLmaxDisruptorQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
        final Queue<IReference<FDate>> requestQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLmaxDisruptorQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LmaxDisruptorQueue<IReference<FDate>>(256,
                true);
        final BlockingQueue<IReference<FDate>> requestQueue = new LmaxDisruptorQueue<IReference<FDate>>(256,
                true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLmaxDisruptorPerformance() throws InterruptedException {
        final RingBuffer<IMutableReference<FDate>> responseQueue = RingBuffer
                .createSingleProducer(() -> new MutableReference<FDate>(), Integers.pow(2, 8));
        final RingBuffer<IMutableReference<FDate>> requestQueue = RingBuffer
                .createSingleProducer(() -> new MutableReference<FDate>(), Integers.pow(2, 8));
        runLmaxPerformanceTest(responseQueue, requestQueue);
    }

    private void runLmaxPerformanceTest(final RingBuffer<IMutableReference<FDate>> responseQueue,
            final RingBuffer<IMutableReference<FDate>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = new LmaxSynchronousWriter<FDate>(responseQueue);
        final ISynchronousReader<FDate> requestReader = new LmaxSynchronousReader<FDate>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = new LmaxSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new LmaxSynchronousReader<FDate>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNioSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testNioDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNioDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new DatagramSocketSynchronousWriter(
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new DatagramSocketSynchronousReader(requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new DatagramSocketSynchronousWriter(requestAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new DatagramSocketSynchronousReader(responseAddress,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testBlockingSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runBlockingSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingSocketSynchronousWriter(
                responseAddress, true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingSocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingSocketSynchronousWriter(requestAddress,
                false, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingSocketSynchronousReader(responseAddress,
                false, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testBlockingDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runBlockingDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingDatagramSocketSynchronousWriter(
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingDatagramSocketSynchronousReader(
                requestAddress, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingDatagramSocketSynchronousWriter(
                requestAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingDatagramSocketSynchronousReader(
                responseAddress, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testAeronDatagramSocketPerformance() throws InterruptedException {
        final String responseChannel = "aeron:udp?endpoint=localhost:7878";
        final String requestChannel = "aeron:udp?endpoint=localhost:7879";
        runAeronPerformanceTest(responseChannel, 1001, requestChannel, 1002);
    }

    @Test
    public void testAeronIpcPerformance() throws InterruptedException {
        final String responseChannel = "aeron:ipc";
        final String requestChannel = "aeron:ipc";
        runAeronPerformanceTest(responseChannel, 1001, requestChannel, 1002);
    }

    private void runAeronPerformanceTest(final String responseChannel, final int responseStreamId,
            final String requestChannel, final int requestStreamId) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new AeronSynchronousWriter(responseChannel,
                responseStreamId);
        final ISynchronousReader<IByteBuffer> requestReader = new AeronSynchronousReader(requestChannel,
                requestStreamId);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAeronPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new AeronSynchronousWriter(requestChannel,
                requestStreamId);
        final ISynchronousReader<IByteBuffer> responseReader = new AeronSynchronousReader(responseChannel,
                responseStreamId);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testKryonetTcpPerformance() throws InterruptedException {
        runKryonetPerformanceTest(Addresses.asAddress("localhost"), 7878, -1, 7879, -1);
    }

    @Test
    public void testKryonetUdpPerformance() throws InterruptedException {
        runKryonetPerformanceTest(Addresses.asAddress("localhost"), 7878, 7878, 8879, 8879);
    }

    private void runKryonetPerformanceTest(final InetAddress address, final int responseTcpPort,
            final int responseUdpPort, final int requestTcpPort, final int requestUdpPort) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new KryonetSynchronousWriter(address,
                responseTcpPort, responseUdpPort, true);
        final ISynchronousReader<IByteBuffer> requestReader = new KryonetSynchronousReader(address, requestTcpPort,
                requestUdpPort, false);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKryonetPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new KryonetSynchronousWriter(address,
                requestTcpPort, requestUdpPort, true);
        final ISynchronousReader<IByteBuffer> responseReader = new KryonetSynchronousReader(address, responseTcpPort,
                responseUdpPort, false);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testJeromqTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://localhost:7878";
        final String requestChannel = "tcp://localhost:7879";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://localhost:7878";
        final String requestChannel = "tcp://localhost:7879";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJeromqInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJeromqPerformanceTest(JeromqSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runJeromqPerformanceTest(final IJeromqSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new JeromqSynchronousWriter(socketType,
                responseChannel, true);
        final ISynchronousReader<IByteBuffer> requestReader = new JeromqSynchronousReader(socketType, requestChannel,
                true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new JeromqSynchronousWriter(socketType,
                requestChannel, false);
        final ISynchronousReader<IByteBuffer> responseReader = new JeromqSynchronousReader(socketType, responseChannel,
                false);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    private <T> ISynchronousReader<T> maybeSynchronize(final ISynchronousReader<T> reader, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(reader, synchronize);
        } else {
            return reader;
        }
    }

    private <T> ISynchronousWriter<T> maybeSynchronize(final ISynchronousWriter<T> writer, final Object synchronize) {
        if (synchronize != null) {
            return SynchronousChannels.synchronize(writer, synchronize);
        } else {
            return writer;
        }
    }

    private void runPerformanceTest(final FileChannelType pipes, final File requestFile, final File responseFile,
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

    private ISynchronousReader<IByteBuffer> newReader(final File file, final FileChannelType pipes) {
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

    private ISynchronousWriter<IByteBufferWriter> newWriter(final File file, final FileChannelType pipes) {
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

    private ISynchronousReader<FDate> newCommandReader(final ISynchronousReader<IByteBuffer> reader) {
        return new SerdeSynchronousReader<FDate>(reader, FDateSerde.GET);
    }

    private ISynchronousWriter<FDate> newCommandWriter(final ISynchronousWriter<IByteBufferWriter> writer) {
        return new SerdeSynchronousWriter<FDate>(writer, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    private void read(final ISynchronousWriter<FDate> requestWriter, final ISynchronousReader<FDate> responseReader) {

        final Instant readsStart = new Instant();
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
                    log.info("client response in");
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

    private void printProgress(final String action, final Instant start, final int count, final int maxCount) {
        final Duration duration = start.toDuration();
        log.info("%s: %s/%s (%s) %s during %s", action, count, maxCount,
                new Percent(count, maxCount).toString(PercentScale.PERCENT),
                new ProcessedEventsRateString(count, duration), duration);
    }

    private ICloseableIterable<FDate> newValues() {
        return FDates.iterable(FDate.MIN_DATE, FDate.MIN_DATE.addMilliseconds(VALUES - 1), FTimeUnit.MILLISECONDS, 1);
    }

    private class WriterTask implements Runnable {

        private final ISynchronousReader<FDate> requestReader;
        private final ISynchronousWriter<FDate> responseWriter;

        WriterTask(final ISynchronousReader<FDate> requestReader, final ISynchronousWriter<FDate> responseWriter) {
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
                final Instant writesStart = new Instant();
                int i = 0;
                if (DEBUG) {
                    log.info("server open request reader");
                }
                requestReader.open();
                if (DEBUG) {
                    log.info("server open response writer");
                }
                responseWriter.open();
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
                        log.info("server response out");
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

}
