package de.invesdwin.context.integration.channel.sync.conversant;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import com.conversantmedia.util.concurrent.ConcurrentQueue;
import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.MultithreadConcurrentQueue;
import com.conversantmedia.util.concurrent.PushPullBlockingQueue;
import com.conversantmedia.util.concurrent.PushPullConcurrentQueue;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ConversantChannelTest extends ALatencyChannelTest {

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
        executor.execute(new LatencyServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = new ConversantSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new ConversantSynchronousReader<FDate>(responseQueue);
        new LatencyClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
