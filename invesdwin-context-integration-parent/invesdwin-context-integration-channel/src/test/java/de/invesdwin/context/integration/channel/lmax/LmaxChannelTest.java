package de.invesdwin.context.integration.channel.lmax;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class LmaxChannelTest extends AChannelTest {

    @Test
    public void testLmaxDisruptorQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
        final Queue<IReference<FDate>> requestQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
        runQueuePerformanceTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLmaxDisruptorQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
        final BlockingQueue<IReference<FDate>> requestQueue = new LmaxDisruptorQueue<IReference<FDate>>(256, true);
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

}
