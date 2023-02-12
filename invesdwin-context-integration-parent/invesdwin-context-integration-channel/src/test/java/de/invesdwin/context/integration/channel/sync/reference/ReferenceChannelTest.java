package de.invesdwin.context.integration.channel.sync.reference;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.reference.AtomicReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.LockedReference;
import de.invesdwin.util.concurrent.reference.SynchronizedReference;
import de.invesdwin.util.concurrent.reference.VolatileReference;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class ReferenceChannelTest extends AChannelTest {

    @Test
    public void testSynchronizedReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new SynchronizedReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new SynchronizedReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testLockedReferencePerformance() throws InterruptedException {
        final ILock lock = ILockCollectionFactory.getInstance(true).newLock("asdf");
        final IMutableReference<IReference<FDate>> responseQueue = new LockedReference<IReference<FDate>>(lock);
        final IMutableReference<IReference<FDate>> requestQueue = new LockedReference<IReference<FDate>>(lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    public void testJavaLockedReferencePerformance() throws InterruptedException {
        //CHECKSTYLE:OFF
        final Lock lock = new ReentrantLock();
        //CHECKSTYLE:ON
        final IMutableReference<IReference<FDate>> responseQueue = new LockedReference<IReference<FDate>>(lock);
        final IMutableReference<IReference<FDate>> requestQueue = new LockedReference<IReference<FDate>>(lock);
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testAtomicReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new AtomicReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new AtomicReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    @Test
    public void testVolatileReferencePerformance() throws InterruptedException {
        final IMutableReference<IReference<FDate>> responseQueue = new VolatileReference<IReference<FDate>>();
        final IMutableReference<IReference<FDate>> requestQueue = new VolatileReference<IReference<FDate>>();
        runReferencePerformanceTest(responseQueue, requestQueue);
    }

    private void runReferencePerformanceTest(final IMutableReference<IReference<FDate>> responseQueue,
            final IMutableReference<IReference<FDate>> requestQueue) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = new ReferenceSynchronousWriter<FDate>(responseQueue);
        final ISynchronousReader<FDate> requestReader = new ReferenceSynchronousReader<FDate>(requestQueue);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runReferencePerformanceTest", 1);
        executor.execute(new WriterTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = new ReferenceSynchronousWriter<FDate>(requestQueue);
        final ISynchronousReader<FDate> responseReader = new ReferenceSynchronousReader<FDate>(responseQueue);
        read(requestWriter, responseReader);
        executor.shutdown();
        executor.awaitTermination();
    }

}
