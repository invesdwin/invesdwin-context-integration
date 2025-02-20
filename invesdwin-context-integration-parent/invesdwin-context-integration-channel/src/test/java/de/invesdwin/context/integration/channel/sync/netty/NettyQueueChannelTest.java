package de.invesdwin.context.integration.channel.sync.netty;

import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class NettyQueueChannelTest extends AChannelTest {

    @Test
    public void testNettySpscQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = io.netty.util.internal.PlatformDependent.newSpscQueue();
        final Queue<IReference<FDate>> requestQueue = io.netty.util.internal.PlatformDependent.newSpscQueue();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testNettyMpscQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = io.netty.util.internal.PlatformDependent.newMpscQueue();
        final Queue<IReference<FDate>> requestQueue = io.netty.util.internal.PlatformDependent.newMpscQueue();
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Test
    public void testNettyFixedMpscQueuePerformance() throws InterruptedException {
        final Queue<IReference<FDate>> responseQueue = io.netty.util.internal.PlatformDependent.newFixedMpscQueue(256);
        final Queue<IReference<FDate>> requestQueue = io.netty.util.internal.PlatformDependent.newFixedMpscQueue(256);
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

}
