package de.invesdwin.context.integration.channel.queue;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.time.date.FDate;

// CHECKSTYLE:OFF
@NotThreadSafe
//@Ignore("manual test")
public class QueueChannelTest extends AChannelTest {
    //CHECKSTYLE:ON

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

}
