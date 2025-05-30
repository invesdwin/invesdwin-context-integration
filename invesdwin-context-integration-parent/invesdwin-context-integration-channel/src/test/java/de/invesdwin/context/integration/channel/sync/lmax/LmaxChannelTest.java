package de.invesdwin.context.integration.channel.sync.lmax;

import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
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
        new LatencyChannelTest(this).runQueueLatencyTest(responseQueue, requestQueue, null, null);
    }

    @Disabled("flakey test")
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
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = new LmaxSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new LmaxSynchronousReader<FDate>(responseQueue);
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

}
