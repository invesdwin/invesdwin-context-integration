package de.invesdwin.context.integration.channel.sync.jctools;

import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

import org.jctools.queues.SpscArrayQueue;
import org.jctools.queues.SpscLinkedQueue;
import org.jctools.queues.atomic.SpscAtomicArrayQueue;
import org.jctools.queues.atomic.SpscLinkedAtomicQueue;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class JctoolsChannelTest extends AChannelTest {

    @Test
    public void testJctoolsSpscLinkedAtomicQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscLinkedAtomicQueue<IReference<FDate>>();
        final Queue<IReference<FDate>> requestQueue = new SpscLinkedAtomicQueue<IReference<FDate>>();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscLinkedQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscLinkedQueue<IReference<FDate>>();
        final Queue<IReference<FDate>> requestQueue = new SpscLinkedQueue<IReference<FDate>>();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscAtomicArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscAtomicArrayQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new SpscAtomicArrayQueue<IReference<FDate>>(2);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testJctoolsSpscArrayQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = new SpscArrayQueue<IReference<FDate>>(2);
        final Queue<IReference<FDate>> requestQueue = new SpscArrayQueue<IReference<FDate>>(2);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

}
