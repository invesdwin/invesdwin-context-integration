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
import de.invesdwin.context.integration.channel.command.CommandSynchronousReader;
import de.invesdwin.context.integration.channel.command.CommandSynchronousWriter;
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
import de.invesdwin.util.concurrent.reference.JavaLockedReference;
import de.invesdwin.util.concurrent.reference.LockedReference;
import de.invesdwin.util.concurrent.reference.SynchronizedReference;
import de.invesdwin.util.concurrent.reference.VolatileReference;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.ProcessedEventsRateString;
import de.invesdwin.util.lang.buffer.IByteBuffer;
import de.invesdwin.util.lang.uri.Addresses;
import de.invesdwin.util.marshallers.serde.basic.FDateSerde;
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
    private static final int VALUES = DEBUG ? 10 : 1_000_000;
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
            final ISynchronousWriter<IByteBuffer> responseWriter = new ChronicleSynchronousWriter(responseFile);
            final ISynchronousReader<IByteBuffer> requestReader = new ChronicleSynchronousReader(requestFile);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(requestReader, responseWriter));
            final ISynchronousWriter<IByteBuffer> requestWriter = new ChronicleSynchronousWriter(requestFile);
            final ISynchronousReader<IByteBuffer> responseReader = new ChronicleSynchronousReader(responseFile);
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
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new ArrayDeque<ISynchronousCommand<IByteBuffer>>(
                1);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new ArrayDeque<ISynchronousCommand<IByteBuffer>>(
                1);
        runQueuePerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new LinkedBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new LinkedBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new LinkedBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new LinkedBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1, false);
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1, false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformanceWithBlockingFair() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1, true);
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new ArrayBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1, true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformance() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new LinkedTransferQueue<ISynchronousCommand<IByteBuffer>>();
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new LinkedTransferQueue<ISynchronousCommand<IByteBuffer>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new LinkedTransferQueue<ISynchronousCommand<IByteBuffer>>();
        final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new LinkedTransferQueue<ISynchronousCommand<IByteBuffer>>();
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test(expected = AssertionError.class)
    public void testSynchronousQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                false);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                false);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlocking() throws InterruptedException {
        final SynchronousQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                false);
        final SynchronousQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                false);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformanceWithBlockingFair() throws InterruptedException {
        final SynchronousQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                true);
        final SynchronousQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new SynchronousQueue<ISynchronousCommand<IByteBuffer>>(
                true);
        runBlockingQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaOneToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new OneToOneConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new OneToOneConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToOneConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new ManyToOneConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new ManyToOneConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testAgronaManyToManyConcurrentArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new ManyToManyConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new ManyToManyConcurrentArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscAtomicArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new SpscAtomicArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new SpscAtomicArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedAtomicQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new SpscLinkedAtomicQueue<ISynchronousCommand<IByteBuffer>>();
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new SpscLinkedAtomicQueue<ISynchronousCommand<IByteBuffer>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new SpscLinkedQueue<ISynchronousCommand<IByteBuffer>>();
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new SpscLinkedQueue<ISynchronousCommand<IByteBuffer>>();
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscArrayQueuePerformance() throws InterruptedException {
        final Queue<ISynchronousCommand<IByteBuffer>> responseQueue = new SpscArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        final Queue<ISynchronousCommand<IByteBuffer>> requestQueue = new SpscArrayQueue<ISynchronousCommand<IByteBuffer>>(
                2);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    private void runQueuePerformanceTest(final Queue<ISynchronousCommand<IByteBuffer>> responseQueue,
            final Queue<ISynchronousCommand<IByteBuffer>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<IByteBuffer> responseWriter = maybeSynchronize(
                new QueueSynchronousWriter<IByteBuffer>(responseQueue), synchronizeResponse);
        final ISynchronousReader<IByteBuffer> requestReader = maybeSynchronize(
                new QueueSynchronousReader<IByteBuffer>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = maybeSynchronize(
                new QueueSynchronousWriter<IByteBuffer>(requestQueue), synchronizeRequest);
        final ISynchronousReader<IByteBuffer> responseReader = maybeSynchronize(
                new QueueSynchronousReader<IByteBuffer>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    private void runBlockingQueuePerformanceTest(final BlockingQueue<ISynchronousCommand<IByteBuffer>> responseQueue,
            final BlockingQueue<ISynchronousCommand<IByteBuffer>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse) throws InterruptedException {
        final ISynchronousWriter<IByteBuffer> responseWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<IByteBuffer>(responseQueue), synchronizeResponse);
        final ISynchronousReader<IByteBuffer> requestReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<IByteBuffer>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBlockingQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = maybeSynchronize(
                new BlockingQueueSynchronousWriter<IByteBuffer>(requestQueue), synchronizeRequest);
        final ISynchronousReader<IByteBuffer> responseReader = maybeSynchronize(
                new BlockingQueueSynchronousReader<IByteBuffer>(responseQueue), synchronizeResponse);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testSynchronizedReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue = new SynchronizedReference<ISynchronousCommand<IByteBuffer>>();
        final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue = new SynchronizedReference<ISynchronousCommand<IByteBuffer>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testLockedReferencePerformance() throws InterruptedException {
        final ILock lock = ILockCollectionFactory.getInstance(true).newLock("asdf");
        final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue = new LockedReference<ISynchronousCommand<IByteBuffer>>(
                lock);
        final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue = new LockedReference<ISynchronousCommand<IByteBuffer>>(
                lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testJavaLockedReferencePerformance() throws InterruptedException {
        //CHECKSTYLE:OFF
        final Lock lock = new ReentrantLock();
        //CHECKSTYLE:ON
        final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue = new JavaLockedReference<ISynchronousCommand<IByteBuffer>>(
                lock);
        final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue = new JavaLockedReference<ISynchronousCommand<IByteBuffer>>(
                lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testAtomicReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue = new AtomicReference<ISynchronousCommand<IByteBuffer>>();
        final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue = new AtomicReference<ISynchronousCommand<IByteBuffer>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testVolatileReferencePerformance() throws InterruptedException {
        final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue = new VolatileReference<ISynchronousCommand<IByteBuffer>>();
        final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue = new VolatileReference<ISynchronousCommand<IByteBuffer>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    private void runReferencePerformanceTest(final IMutableReference<ISynchronousCommand<IByteBuffer>> responseQueue,
            final IMutableReference<ISynchronousCommand<IByteBuffer>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<IByteBuffer> responseWriter = new ReferenceSynchronousWriter<IByteBuffer>(
                responseQueue);
        final ISynchronousReader<IByteBuffer> requestReader = new ReferenceSynchronousReader<IByteBuffer>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new ReferenceSynchronousWriter<IByteBuffer>(requestQueue);
        final ISynchronousReader<IByteBuffer> responseReader = new ReferenceSynchronousReader<IByteBuffer>(
                responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testConversantPushPullConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new PushPullConcurrentQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new PushPullConcurrentQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantPushPullBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new PushPullBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new PushPullBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                1);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorConcurrentPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new MultithreadConcurrentQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new MultithreadConcurrentQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testConversantDisruptorBlockingPerformance() throws InterruptedException {
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> responseQueue = new DisruptorBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> requestQueue = new DisruptorBlockingQueue<ISynchronousCommand<IByteBuffer>>(
                256);
        runConversantPerformanceTest(responseQueue, requestQueue);
    }

    private void runConversantPerformanceTest(final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> responseQueue,
            final ConcurrentQueue<ISynchronousCommand<IByteBuffer>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<IByteBuffer> responseWriter = new ConversantSynchronousWriter<IByteBuffer>(
                responseQueue);
        final ISynchronousReader<IByteBuffer> requestReader = new ConversantSynchronousReader<IByteBuffer>(
                requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new ConversantSynchronousWriter<IByteBuffer>(
                requestQueue);
        final ISynchronousReader<IByteBuffer> responseReader = new ConversantSynchronousReader<IByteBuffer>(
                responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testLmaxDisruptorPerformance() throws InterruptedException {
        final RingBuffer<MutableSynchronousCommand<IByteBuffer>> responseQueue = RingBuffer
                .createSingleProducer(() -> new MutableSynchronousCommand<IByteBuffer>(), Integers.pow(2, 8));
        final RingBuffer<MutableSynchronousCommand<IByteBuffer>> requestQueue = RingBuffer
                .createSingleProducer(() -> new MutableSynchronousCommand<IByteBuffer>(), Integers.pow(2, 8));
        runLmaxPerformanceTest(responseQueue, requestQueue);
    }

    private void runLmaxPerformanceTest(final RingBuffer<MutableSynchronousCommand<IByteBuffer>> responseQueue,
            final RingBuffer<MutableSynchronousCommand<IByteBuffer>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<IByteBuffer> responseWriter = new LmaxSynchronousWriter<IByteBuffer>(responseQueue);
        final ISynchronousReader<IByteBuffer> requestReader = new LmaxSynchronousReader<IByteBuffer>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testQueuePerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new LmaxSynchronousWriter<IByteBuffer>(requestQueue);
        final ISynchronousReader<IByteBuffer> responseReader = new LmaxSynchronousReader<IByteBuffer>(responseQueue);
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
        final ISynchronousWriter<IByteBuffer> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
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
        final ISynchronousWriter<IByteBuffer> responseWriter = new DatagramSocketSynchronousWriter(responseAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new DatagramSocketSynchronousReader(requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new DatagramSocketSynchronousWriter(requestAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new DatagramSocketSynchronousReader(responseAddress,
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
        final ISynchronousWriter<IByteBuffer> responseWriter = new AeronSynchronousWriter(responseChannel,
                responseStreamId);
        final ISynchronousReader<IByteBuffer> requestReader = new AeronSynchronousReader(requestChannel,
                requestStreamId);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAeronPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new AeronSynchronousWriter(requestChannel,
                requestStreamId);
        final ISynchronousReader<IByteBuffer> responseReader = new AeronSynchronousReader(responseChannel,
                responseStreamId);
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
        final ISynchronousWriter<IByteBuffer> responseWriter = new KryonetSynchronousWriter(address, responseTcpPort,
                responseUdpPort, true);
        final ISynchronousReader<IByteBuffer> requestReader = new KryonetSynchronousReader(address, requestTcpPort,
                requestUdpPort, false);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKryonetPerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<IByteBuffer> requestWriter = new KryonetSynchronousWriter(address, requestTcpPort,
                requestUdpPort, true);
        final ISynchronousReader<IByteBuffer> responseReader = new KryonetSynchronousReader(address, responseTcpPort,
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
        final ISynchronousWriter<IByteBuffer> responseWriter = new JeromqSynchronousWriter(socketType, responseChannel,
                true);
        final ISynchronousReader<IByteBuffer> requestReader = new JeromqSynchronousReader(socketType, requestChannel,
                true);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runJeromqPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBuffer> requestWriter = new JeromqSynchronousWriter(socketType, requestChannel,
                false);
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
            final ISynchronousWriter<IByteBuffer> responseWriter = maybeSynchronize(newWriter(responseFile, pipes),
                    synchronizeResponse);
            final ISynchronousReader<IByteBuffer> requestReader = maybeSynchronize(newReader(requestFile, pipes),
                    synchronizeRequest);
            final WrappedExecutorService executor = Executors.newFixedThreadPool(responseFile.getName(), 1);
            executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
            final ISynchronousWriter<IByteBuffer> requestWriter = maybeSynchronize(newWriter(requestFile, pipes),
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
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousReader(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousReader(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private ISynchronousWriter<IByteBuffer> newWriter(final File file, final FileChannelType pipes) {
        if (pipes == FileChannelType.PIPE) {
            return new PipeSynchronousWriter(file, MESSAGE_SIZE);
        } else if (pipes == FileChannelType.MAPPED) {
            return new MappedSynchronousWriter(file, MESSAGE_SIZE);
        } else {
            throw UnknownArgumentException.newInstance(FileChannelType.class, pipes);
        }
    }

    private ISynchronousReader<ISynchronousCommand<FDate>> newCommandReader(
            final ISynchronousReader<IByteBuffer> reader) {
        return new CommandSynchronousReader<FDate>(reader, FDateSerde.GET);
    }

    private ISynchronousWriter<ISynchronousCommand<FDate>> newCommandWriter(
            final ISynchronousWriter<IByteBuffer> writer) {
        return new CommandSynchronousWriter<FDate>(writer, FDateSerde.GET, FDateSerde.FIXED_LENGTH);
    }

    private void read(final ISynchronousWriter<ISynchronousCommand<FDate>> requestWriter,
            final ISynchronousReader<ISynchronousCommand<FDate>> responseReader) {

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
            final MutableSynchronousCommand<FDate> command = new MutableSynchronousCommand<>();
            while (true) {
                command.setType(MESSAGE_TYPE);
                command.setSequence(MESSAGE_SEQUENCE);
                command.setMessage(null);
                requestWriter.write(command);
                if (DEBUG) {
                    log.info("client request out");
                }
                Assertions.checkTrue(spinWait.awaitFulfill(waitingSinceNanos, MAX_WAIT_DURATION));
                final ISynchronousCommand<FDate> readMessage = responseReader.readMessage();
                if (DEBUG) {
                    log.info("client response in");
                }
                final int messageType = readMessage.getType();
                final int messageSequence = readMessage.getSequence();
                Assertions.checkEquals(messageType, MESSAGE_TYPE);
                Assertions.checkEquals(messageSequence, MESSAGE_SEQUENCE);
                final FDate value = readMessage.getMessage();
                readMessage.close(); //free memory
                Assertions.checkNotNull(value);
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

        private final ISynchronousReader<ISynchronousCommand<FDate>> requestReader;
        private final ISynchronousWriter<ISynchronousCommand<FDate>> responseWriter;

        WriterTask(final ISynchronousReader<ISynchronousCommand<FDate>> requestReader,
                final ISynchronousWriter<ISynchronousCommand<FDate>> responseWriter) {
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
                final MutableSynchronousCommand<FDate> command = new MutableSynchronousCommand<>();
                for (final FDate date : newValues()) {
                    Assertions.checkTrue(spinWait.awaitFulfill(waitingSinceNanos, MAX_WAIT_DURATION));
                    if (DEBUG) {
                        log.info("server request in");
                    }
                    final ISynchronousCommand<FDate> readMessage = requestReader.readMessage();
                    Assertions.checkEquals(readMessage.getType(), MESSAGE_TYPE);
                    Assertions.checkEquals(readMessage.getSequence(), MESSAGE_SEQUENCE);
                    Assertions.checkNull(readMessage.getMessage());
                    readMessage.close(); //free memory
                    command.setType(MESSAGE_TYPE);
                    command.setSequence(MESSAGE_SEQUENCE);
                    command.setMessage(date);
                    responseWriter.write(command);
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
