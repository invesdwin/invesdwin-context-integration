package de.invesdwin.context.integration.channel.sync.queue;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class QueueChannelTest extends AChannelTest {

    @Test
    public void testConcurrentLinkedDequePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new ConcurrentLinkedDeque<IReference<FDate>>();
        final Queue<IReference<FDate>> requestQueue = new ConcurrentLinkedDeque<IReference<FDate>>();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedBlockingDequePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new LinkedBlockingDeque<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new LinkedBlockingDeque<IReference<FDate>>(2);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testLinkedBlockingDequePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedBlockingDeque<IReference<FDate>>(2);
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedBlockingDeque<IReference<FDate>>(2);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayDequePerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final Queue<IReference<FDate>> responseQueue = new ArrayDeque<IReference<FDate>>(1);
        final Queue<IReference<FDate>> requestQueue = new ArrayDeque<IReference<FDate>>(1);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, requestQueue, responseQueue);
    }

    @Test
    public void testLinkedBlockingQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new LinkedBlockingQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new LinkedBlockingQueue<IReference<FDate>>(2);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled
    @Test
    public void testLinkedBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedBlockingQueue<IReference<FDate>>(1);
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedBlockingQueue<IReference<FDate>>(1);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testArrayBlockingQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(2);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testArrayBlockingQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(1, false);
        final BlockingQueue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(1, false);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testArrayBlockingQueuePerformanceWithBlockingFair() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new ArrayBlockingQueue<IReference<FDate>>(1, true);
        final BlockingQueue<IReference<FDate>> requestQueue = new ArrayBlockingQueue<IReference<FDate>>(1, true);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testLinkedTransferQueuePerformance() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedTransferQueue<IReference<FDate>>();
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedTransferQueue<IReference<FDate>>();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testLinkedTransferQueuePerformanceWithBlocking() throws InterruptedException {
        final BlockingQueue<IReference<FDate>> responseQueue = new LinkedTransferQueue<IReference<FDate>>();
        final BlockingQueue<IReference<FDate>> requestQueue = new LinkedTransferQueue<IReference<FDate>>();
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testSynchronousQueuePerformance() throws InterruptedException {
        Assertions.assertThrows(AssertionError.class, () -> {
            final Queue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(false);
            final Queue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(false);
            new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
        });
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testSynchronousQueuePerformanceWithBlocking() throws InterruptedException {
        final SynchronousQueue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(false);
        final SynchronousQueue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(false);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("causes cpu spikes")
    @Test
    public void testSynchronousQueuePerformanceWithBlockingFair() throws InterruptedException {
        final SynchronousQueue<IReference<FDate>> responseQueue = new SynchronousQueue<IReference<FDate>>(true);
        final SynchronousQueue<IReference<FDate>> requestQueue = new SynchronousQueue<IReference<FDate>>(true);
        new LatencyChannelTest(this).runBlockingQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

}
