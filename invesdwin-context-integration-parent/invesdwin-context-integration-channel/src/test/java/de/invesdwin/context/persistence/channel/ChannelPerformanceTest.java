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
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.MutableSynchronousCommand;
import de.invesdwin.context.integration.channel.conversant.ConversantSynchronousReader;
import de.invesdwin.context.integration.channel.conversant.ConversantSynchronousWriter;
import de.invesdwin.context.integration.channel.kryonet.KryonetSynchronousReader;
import de.invesdwin.context.integration.channel.kryonet.KryonetSynchronousWriter;
import de.invesdwin.context.integration.channel.lmax.LmaxSynchronousReader;
import de.invesdwin.context.integration.channel.lmax.LmaxSynchronousWriter;
import de.invesdwin.context.integration.channel.mapped.MappedSynchronousReader;
import de.invesdwin.context.integration.channel.mapped.MappedSynchronousWriter;
import de.invesdwin.context.integration.channel.pipe.PipeSynchronousReader;
import de.invesdwin.context.integration.channel.pipe.PipeSynchronousWriter;
import de.invesdwin.context.integration.channel.queue.QueueSynchronousReader;
import de.invesdwin.context.integration.channel.queue.QueueSynchronousWriter;
import de.invesdwin.context.integration.channel.queue.blocking.BlockingQueueSynchronousReader;
import de.invesdwin.context.integration.channel.queue.blocking.BlockingQueueSynchronousWriter;
import de.invesdwin.context.integration.channel.reference.ReferenceSynchronousReader;
import de.invesdwin.context.integration.channel.reference.ReferenceSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.SocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.SocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.czmq.CzmqSynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.czmq.CzmqSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.CzmqSocketType;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketType;
import de.invesdwin.context.integration.channel.zeromq.jeromq.JeromqSynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.jeromq.JeromqSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.jeromq.type.IJeromqSocketType;
import de.invesdwin.context.integration.channel.zeromq.jeromq.type.JeromqSocketType;
import de.invesdwin.context.integration.channel.zeromq.jzmq.JzmqSynchronousReader;
import de.invesdwin.context.integration.channel.zeromq.jzmq.JzmqSynchronousWriter;
import de.invesdwin.context.integration.channel.zeromq.jzmq.type.IJzmqSocketType;
import de.invesdwin.context.integration.channel.zeromq.jzmq.type.JzmqSocketType;
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
import de.invesdwin.util.concurrent.reference.JavaLockedReference;
import de.invesdwin.util.concurrent.reference.LockedReference;
import de.invesdwin.util.concurrent.reference.SynchronizedReference;
import de.invesdwin.util.concurrent.reference.VolatileReference;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.lang.uri.Addresses;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.decimal.scaled.Percent;
import de.invesdwin.util.math.decimal.scaled.PercentScale;
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

    private static final boolean DEBUG = false;
    private static final int MESSAGE_SIZE = FDateSerde.FIXED_LENGTH;
    private static final int MESSAGE_TYPE = 1;
    private static final int MESSAGE_SEQUENCE = 1;
    private static final int VALUES = DEBUG ? 10 : 10_000_000;
    private static final int FLUSH_INTERVAL = Math.max(10, VALUES / 10);
    private static final Duration MAX_WAIT_DURATION = new Duration(10, DEBUG ? FTimeUnit.DAYS : FTimeUnit.SECONDS);

    private enum FileChannelType {
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
        if (pipes == FileChannelType.PIPE) {
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
            final ISynchronousWriter<byte[]> responseWriter = new ChronicleSynchronousWriter(responseFile);
            final ISynchronousReader<byte[]> requestReader = new ChronicleSynchronousReader(requestFile);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(requestReader, responseWriter));
            final ISynchronousWriter<byte[]> requestWriter = new ChronicleSynchronousWriter(requestFile);
            final ISynchronousReader<byte[]> responseReader = new ChronicleSynchronousReader(responseFile);
            read(requestWriter, responseReader);
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
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new ArrayDeque<ISynchronousCommand<byte[]>>(1);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new ArrayDeque<ISynchronousCommand<byte[]>>(1);
        runQueuePerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new LinkedBlockingQueue<ISynchronousCommand<byte[]>>(
                2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new LinkedBlockingQueue<ISynchronousCommand<byte[]>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue = new LinkedBlockingQueue<ISynchronousCommand<byte[]>>(
                1);
        final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue = new LinkedBlockingQueue<ISynchronousCommand<byte[]>>(
                1);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(
                1, false);
        final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(
                1, false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlockingFair() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(
                1, true);
        final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<byte[]>>(
                1, true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformance() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue = new LinkedTransferQueue<ISynchronousCommand<byte[]>>();
        final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue = new LinkedTransferQueue<ISynchronousCommand<byte[]>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue = new LinkedTransferQueue<ISynchronousCommand<byte[]>>();
        final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue = new LinkedTransferQueue<ISynchronousCommand<byte[]>>();
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test(expected = AssertionError.class)
    public void testSynchronousQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                false);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                false);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlocking() throws InterruptedException {
        final SynchronousQueue<ISynchronousCommand<byte[]>> responseQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                false);
        final SynchronousQueue<ISynchronousCommand<byte[]>> requestQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlockingFair() throws InterruptedException {
        final SynchronousQueue<ISynchronousCommand<byte[]>> responseQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                true);
        final SynchronousQueue<ISynchronousCommand<byte[]>> requestQueue = new SynchronousQueue<ISynchronousCommand<byte[]>>(
                true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaOneToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new OneToOneConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                256);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new OneToOneConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                256);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new ManyToOneConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new ManyToOneConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToManyConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new ManyToManyConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new ManyToManyConcurrentArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscAtomicArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new SpscAtomicArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new SpscAtomicArrayQueue<ISynchronousCommand<byte[]>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedAtomicQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new SpscLinkedAtomicQueue<ISynchronousCommand<byte[]>>();
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new SpscLinkedAtomicQueue<ISynchronousCommand<byte[]>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new SpscLinkedQueue<ISynchronousCommand<byte[]>>();
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new SpscLinkedQueue<ISynchronousCommand<byte[]>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<byte[]>> responseQueue = new SpscArrayQueue<ISynchronousCommand<byte[]>>(2);
        final Queue<ISynchronousCommand<byte[]>> requestQueue = new SpscArrayQueue<ISynchronousCommand<byte[]>>(2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    private void runQueuePerformanceTest(final Queue<ISynchronousCommand<byte[]>> responseQueue,
            final Queue<ISynchronousCommand<byte[]>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = maybeSynchronize(
                new QueueSynchronousWriter<byte[]>(responseQueue), synchronizeResponse);
        final ISynchronousReader<byte[]> requestReader = maybeSynchronize(
                new QueueSynchronousReader<byte[]>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = maybeSynchronize(
                new QueueSynchronousWriter<byte[]>(requestQueue), synchronizeRequest);
        final ISynchronousReader<byte[]> responseReader = maybeSynchronize(
                new QueueSynchronousReader<byte[]>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    private void runBlockingQueuePerformanceTest(final BlockingQueue<ISynchronousCommand<byte[]>> responseQueue,
            final BlockingQueue<ISynchronousCommand<byte[]>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<byte[]>(responseQueue), synchronizeResponse);
        final ISynchronousReader<byte[]> requestReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<byte[]>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBlockingQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<byte[]>(requestQueue), synchronizeRequest);
        final ISynchronousReader<byte[]> responseReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<byte[]>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testSynchronizedReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<byte[]>> responseQueue = new SynchronizedReference<ISynchronousCommand<byte[]>>();
        final IMutableReference<ISynchronousCommand<byte[]>> requestQueue = new SynchronizedReference<ISynchronousCommand<byte[]>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testLockedReferencePerformance() throws InterruptedException {
        final ILock lock = ILockCollectionFactory.getInstance(true).newLock("asdf");
        final IMutableReference<ISynchronousCommand<byte[]>> responseQueue = new LockedReference<ISynchronousCommand<byte[]>>(
                lock);
        final IMutableReference<ISynchronousCommand<byte[]>> requestQueue = new LockedReference<ISynchronousCommand<byte[]>>(
                lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testJavaLockedReferencePerformance() throws InterruptedException {
        //CHECKSTYLE:OFF
        final Lock lock = new ReentrantLock();
        //CHECKSTYLE:ON
        final IMutableReference<ISynchronousCommand<byte[]>> responseQueue = new JavaLockedReference<ISynchronousCommand<byte[]>>(
                lock);
        final IMutableReference<ISynchronousCommand<byte[]>> requestQueue = new JavaLockedReference<ISynchronousCommand<byte[]>>(
                lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testAtomicReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<byte[]>> responseQueue = new AtomicReference<ISynchronousCommand<byte[]>>();
        final IMutableReference<ISynchronousCommand<byte[]>> requestQueue = new AtomicReference<ISynchronousCommand<byte[]>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testVolatileReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<byte[]>> responseQueue = new VolatileReference<ISynchronousCommand<byte[]>>();
        final IMutableReference<ISynchronousCommand<byte[]>> requestQueue = new VolatileReference<ISynchronousCommand<byte[]>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    private void runReferencePerformanceTest(final IMutableReference<ISynchronousCommand<byte[]>> responseQueue,
            final IMutableReference<ISynchronousCommand<byte[]>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new ReferenceSynchronousWriter<byte[]>(responseQueue);
        final ISynchronousReader<byte[]> requestReader = new ReferenceSynchronousReader<byte[]>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new ReferenceSynchronousWriter<byte[]>(requestQueue);
        final ISynchronousReader<byte[]> responseReader = new ReferenceSynchronousReader<byte[]>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testConversantPushPullConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<byte[]>> responseQueue = new PushPullConcurrentQueue<ISynchronousCommand<byte[]>>(
                1);
        final ConcurrentQueue<ISynchronousCommand<byte[]>> requestQueue = new PushPullConcurrentQueue<ISynchronousCommand<byte[]>>(
                1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantPushPullBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<byte[]>> responseQueue = new PushPullBlockingQueue<ISynchronousCommand<byte[]>>(
                1);
        final ConcurrentQueue<ISynchronousCommand<byte[]>> requestQueue = new PushPullBlockingQueue<ISynchronousCommand<byte[]>>(
                1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<byte[]>> responseQueue = new MultithreadConcurrentQueue<ISynchronousCommand<byte[]>>(
                256);
        final ConcurrentQueue<ISynchronousCommand<byte[]>> requestQueue = new MultithreadConcurrentQueue<ISynchronousCommand<byte[]>>(
                256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<byte[]>> responseQueue = new DisruptorBlockingQueue<ISynchronousCommand<byte[]>>(
                256);
        final ConcurrentQueue<ISynchronousCommand<byte[]>> requestQueue = new DisruptorBlockingQueue<ISynchronousCommand<byte[]>>(
                256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    private void runConversantPerformanceTest(final ConcurrentQueue<ISynchronousCommand<byte[]>> responseQueue,
            final ConcurrentQueue<ISynchronousCommand<byte[]>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new ConversantSynchronousWriter<byte[]>(responseQueue);
        final ISynchronousReader<byte[]> requestReader = new ConversantSynchronousReader<byte[]>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new ConversantSynchronousWriter<byte[]>(requestQueue);
        final ISynchronousReader<byte[]> responseReader = new ConversantSynchronousReader<byte[]>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testLmaxDisruptorPerformance() throws InterruptedException {
        final RingBuffer<MutableSynchronousCommand<byte[]>> responseQueue = RingBuffer
                .createSingleProducer(() -> new MutableSynchronousCommand<byte[]>(), Integers.pow(2, 8));
        final RingBuffer<MutableSynchronousCommand<byte[]>> requestQueue = RingBuffer
                .createSingleProducer(() -> new MutableSynchronousCommand<byte[]>(), Integers.pow(2, 8));
        runLmaxPerformanceTest(responseQueue, requestQueue);
    }

    private void runLmaxPerformanceTest(final RingBuffer<MutableSynchronousCommand<byte[]>> responseQueue,
            final RingBuffer<MutableSynchronousCommand<byte[]>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new LmaxSynchronousWriter<byte[]>(responseQueue);
        final ISynchronousReader<byte[]> requestReader = new LmaxSynchronousReader<byte[]>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new LmaxSynchronousWriter<byte[]>(requestQueue);
        final ISynchronousReader<byte[]> responseReader = new LmaxSynchronousReader<byte[]>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                MESSAGE_SIZE);
        final ISynchronousReader<byte[]> requestReader = new SocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                MESSAGE_SIZE);
        final ISynchronousReader<byte[]> responseReader = new SocketSynchronousReader(responseAddress, false,
                MESSAGE_SIZE);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new DatagramSocketSynchronousWriter(responseAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<byte[]> requestReader = new DatagramSocketSynchronousReader(requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new DatagramSocketSynchronousWriter(requestAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<byte[]> responseReader = new DatagramSocketSynchronousReader(responseAddress,
                MESSAGE_SIZE);
        read(requestWriter, responseReader);
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
        final ISynchronousWriter<byte[]> responseWriter = new AeronSynchronousWriter(responseChannel, responseStreamId);
        final ISynchronousReader<byte[]> requestReader = new AeronSynchronousReader(requestChannel, requestStreamId);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAeronPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new AeronSynchronousWriter(requestChannel, requestStreamId);
        final ISynchronousReader<byte[]> responseReader = new AeronSynchronousReader(responseChannel, responseStreamId);
        read(requestWriter, responseReader);
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
        final ISynchronousWriter<byte[]> responseWriter = new KryonetSynchronousWriter(address, responseTcpPort,
                responseUdpPort, true);
        final ISynchronousReader<byte[]> requestReader = new KryonetSynchronousReader(address, requestTcpPort,
                requestUdpPort, false);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKryonetPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new KryonetSynchronousWriter(address, requestTcpPort,
                requestUdpPort, true);
        final ISynchronousReader<byte[]> responseReader = new KryonetSynchronousReader(address, responseTcpPort,
                responseUdpPort, false);
        read(requestWriter, responseReader);
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
        final ISynchronousWriter<byte[]> responseWriter = new JeromqSynchronousWriter(socketType, responseChannel,
                true);
        final ISynchronousReader<byte[]> requestReader = new JeromqSynchronousReader(socketType, requestChannel, true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new JeromqSynchronousWriter(socketType, requestChannel, false);
        final ISynchronousReader<byte[]> responseReader = new JeromqSynchronousReader(socketType, responseChannel,
                false);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testCzmqTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runCzmqPerformanceTest(CzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runCzmqPerformanceTest(CzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runCzmqPerformanceTest(CzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runCzmqPerformanceTest(CzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runCzmqPerformanceTest(CzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runCzmqPerformanceTest(CzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testCzmqInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runCzmqPerformanceTest(CzmqSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runCzmqPerformanceTest(final ICzmqSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new CzmqSynchronousWriter(socketType, responseChannel, true);
        final ISynchronousReader<byte[]> requestReader = new CzmqSynchronousReader(socketType, requestChannel, false);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new CzmqSynchronousWriter(socketType, requestChannel, true);
        final ISynchronousReader<byte[]> responseReader = new CzmqSynchronousReader(socketType, responseChannel, false);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testJzmqTcpPairPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runJzmqPerformanceTest(JzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqTcpPushPullPerformance() throws InterruptedException {
        final String responseChannel = "tcp://127.0.0.1:7878";
        final String requestChannel = "tcp://127.0.0.1:7879";
        runJzmqPerformanceTest(JzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqIpcPairPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJzmqPerformanceTest(JzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqIpcPushPullPerformance() throws InterruptedException {
        final String responseChannel = "ipc://response";
        final String requestChannel = "ipc://request";
        runJzmqPerformanceTest(JzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqInprocPairPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJzmqPerformanceTest(JzmqSocketType.PAIR, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqInprocPushPullPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJzmqPerformanceTest(JzmqSocketType.PUSHPULL, responseChannel, requestChannel);
    }

    @Test
    public void testJzmqInprocPubSubPerformance() throws InterruptedException {
        final String responseChannel = "inproc://response";
        final String requestChannel = "inproc://request";
        runJzmqPerformanceTest(JzmqSocketType.PUBSUB, responseChannel, requestChannel);
    }

    private void runJzmqPerformanceTest(final IJzmqSocketType socketType, final String responseChannel,
            final String requestChannel) throws InterruptedException {
        final ISynchronousWriter<byte[]> responseWriter = new JzmqSynchronousWriter(socketType, responseChannel, true);
        final ISynchronousReader<byte[]> requestReader = new JzmqSynchronousReader(socketType, requestChannel, true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<byte[]> requestWriter = new JzmqSynchronousWriter(socketType, requestChannel, false);
        final ISynchronousReader<byte[]> responseReader = new JzmqSynchronousReader(socketType, responseChannel, false);
        read(requestWriter, responseReader);
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
            final ISynchronousWriter<byte[]> responseWriter = maybeSynchronize(newWriter(responseFile, pipes),
                    synchronizeResponse);
            final ISynchronousReader<byte[]> requestReader = maybeSynchronize(newReader(requestFile, pipes),
                    synchronizeRequest);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(requestReader, responseWriter));
            final ISynchronousWriter<byte[]> requestWriter = maybeSynchronize(newWriter(requestFile, pipes),
                    synchronizeRequest);
            final ISynchronousReader<byte[]> responseReader = maybeSynchronize(newReader(responseFile, pipes),
                    synchronizeResponse);
            read(requestWriter, responseReader);
            executor.shutdown();
            executor.awaitTermination();
        } finally {
            Files.deleteQuietly(requestFile);
            Files.deleteQuietly(responseFile);
        }
    }

    private ISynchronousReader<byte[]> newReader(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private ISynchronousWriter<byte[]> newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private void read(final ISynchronousWriter<byte[]> requestWriter, final ISynchronousReader<byte[]> responseReader) {
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
                requestWriter.write(MESSAGE_TYPE, MESSAGE_SEQUENCE, Bytes.EMPTY_ARRAY);
                if (DEBUG) {
                    log.info("client request out");
                }
                Assertions.checkTrue(spinWait.awaitFulfill(waitingSinceNanos, MAX_WAIT_DURATION));
                final ISynchronousCommand<byte[]> readMessage = responseReader.readMessage();
                if (DEBUG) {
                    log.info("client response in");
                }
                final int messageType = readMessage.getType();
                final int messageSequence = readMessage.getSequence();
                final byte[] responseBytes = readMessage.getMessage();
                Assertions.checkEquals(messageType, MESSAGE_TYPE);
                Assertions.checkEquals(messageSequence, MESSAGE_SEQUENCE);
                Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                final FDate value = FDateSerde.GET.fromBytes(responseBytes);
                if (prevValue != null) {
                    Assertions.checkTrue(prevValue.isBefore(value));
                }
                prevValue = value;
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

        private final ISynchronousReader<byte[]> requestReader;
        private final ISynchronousWriter<byte[]> responseWriter;

        WriterTask(final ISynchronousReader<byte[]> requestReader, final ISynchronousWriter<byte[]> responseWriter) {
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
                    final ISynchronousCommand<byte[]> readMessage = requestReader.readMessage();
                    Assertions.checkEquals(readMessage.getType(), MESSAGE_TYPE);
                    Assertions.checkEquals(readMessage.getSequence(), MESSAGE_SEQUENCE);
                    Assertions.checkEquals(readMessage.getMessage().length, 0);
                    final byte[] responseBytes = FDateSerde.GET.toBytes(date);
                    Assertions.checkEquals(responseBytes.length, MESSAGE_SIZE);
                    responseWriter.write(MESSAGE_TYPE, MESSAGE_SEQUENCE, responseBytes);
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
