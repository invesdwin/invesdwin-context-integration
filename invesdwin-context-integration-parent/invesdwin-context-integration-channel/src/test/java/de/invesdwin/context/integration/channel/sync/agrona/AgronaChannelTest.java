package de.invesdwin.context.integration.channel.sync.agrona;

import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.agrona.broadcast.BroadcastSynchronousReader;
import de.invesdwin.context.integration.channel.sync.agrona.broadcast.BroadcastSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.agrona.ringbuffer.RingBufferSynchronousReader;
import de.invesdwin.context.integration.channel.sync.agrona.ringbuffer.RingBufferSynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class AgronaChannelTest extends AChannelTest {

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
    public void testAgronaOneToOneRingBufferPerformance() throws InterruptedException {
        final int bufferSize = 4096 + RingBufferDescriptor.TRAILER_LENGTH;
        final boolean zeroCopy = false;
        final org.agrona.concurrent.ringbuffer.RingBuffer responseChannel = new OneToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        final org.agrona.concurrent.ringbuffer.RingBuffer requestChannel = new OneToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        runAgronaRingBufferPerformanceTest(responseChannel, requestChannel, zeroCopy);
    }

    @Test
    public void testAgronaOneToOneRingBufferPerformanceWithZeroCopy() throws InterruptedException {
        final int bufferSize = 4096 + RingBufferDescriptor.TRAILER_LENGTH;
        final boolean zeroCopy = true;
        final org.agrona.concurrent.ringbuffer.RingBuffer responseChannel = new OneToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        final org.agrona.concurrent.ringbuffer.RingBuffer requestChannel = new OneToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        runAgronaRingBufferPerformanceTest(responseChannel, requestChannel, zeroCopy);
    }

    @Test
    public void testAgronaManyToOneRingBufferPerformance() throws InterruptedException {
        final int bufferSize = 4096 + RingBufferDescriptor.TRAILER_LENGTH;
        final boolean zeroCopy = false;
        final org.agrona.concurrent.ringbuffer.RingBuffer responseChannel = new ManyToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        final org.agrona.concurrent.ringbuffer.RingBuffer requestChannel = new ManyToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        runAgronaRingBufferPerformanceTest(responseChannel, requestChannel, zeroCopy);
    }

    @Disabled
    @Test
    public void testAgronaManyToOneRingBufferPerformanceWithZeroCopy() throws InterruptedException {
        final int bufferSize = 4096 + RingBufferDescriptor.TRAILER_LENGTH;
        final boolean zeroCopy = true;
        final org.agrona.concurrent.ringbuffer.RingBuffer responseChannel = new ManyToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        final org.agrona.concurrent.ringbuffer.RingBuffer requestChannel = new ManyToOneRingBuffer(
                new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize)));
        runAgronaRingBufferPerformanceTest(responseChannel, requestChannel, zeroCopy);
    }

    private void runAgronaRingBufferPerformanceTest(final org.agrona.concurrent.ringbuffer.RingBuffer responseChannel,
            final org.agrona.concurrent.ringbuffer.RingBuffer requestChannel, final boolean zeroCopy)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new RingBufferSynchronousWriter(responseChannel,
                zeroCopy ? getMaxMessageSize() : null);
        final ISynchronousReader<IByteBufferProvider> requestReader = new RingBufferSynchronousReader(requestChannel,
                zeroCopy);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAgronaRingBufferPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new RingBufferSynchronousWriter(requestChannel,
                zeroCopy ? getMaxMessageSize() : null);
        final ISynchronousReader<IByteBufferProvider> responseReader = new RingBufferSynchronousReader(responseChannel,
                zeroCopy);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testAgronaBroadcastPerformance() throws InterruptedException {
        final int bufferSize = 4096 + BroadcastBufferDescriptor.TRAILER_LENGTH;
        final AtomicBuffer responseChannel = new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize));
        final AtomicBuffer requestChannel = new UnsafeBuffer(java.nio.ByteBuffer.allocate(bufferSize));
        runAgronaBroadcastPerformanceTest(responseChannel, requestChannel);
    }

    private void runAgronaBroadcastPerformanceTest(final AtomicBuffer responseChannel,
            final AtomicBuffer requestChannel) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new BroadcastSynchronousWriter(responseChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new BroadcastSynchronousReader(requestChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runAgronaBroadcastPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BroadcastSynchronousWriter(requestChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new BroadcastSynchronousReader(responseChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
