package de.invesdwin.context.integration.channel.sync.bufferingiterator;

import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator.INode;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.time.date.FDate;

// CHECKSTYLE:OFF
@NotThreadSafe
public class BufferingIteratorChannelTest extends AChannelTest {
    //CHECKSTYLE:ON

    private static final class MutableReferenceNode extends MutableReference<FDate>
            implements INode<MutableReferenceNode> {

        private MutableReferenceNode next;

        private MutableReferenceNode(final FDate value) {
            super(value);
        }

        @Override
        public MutableReferenceNode getNext() {
            return next;
        }

        @Override
        public void setNext(final MutableReferenceNode next) {
            this.next = next;
        }

    }

    @Test
    public void testNodeBufferingIteratorPerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final IBufferingIterator<MutableReferenceNode> responseQueue = new NodeBufferingIterator<MutableReferenceNode>();
        final IBufferingIterator<MutableReferenceNode> requestQueue = new NodeBufferingIterator<MutableReferenceNode>();
        runBufferingIteratorPerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue,
                message -> new MutableReferenceNode(message));
    }

    @Test
    public void testBufferingIteratorPerformance() throws InterruptedException {
        //ArrayDeque is not threadsafe, thus requires manual synchronization
        final IBufferingIterator<IReference<FDate>> responseQueue = new BufferingIterator<IReference<FDate>>();
        final IBufferingIterator<IReference<FDate>> requestQueue = new BufferingIterator<IReference<FDate>>();
        runBufferingIteratorPerformanceTest(responseQueue, requestQueue, requestQueue, responseQueue,
                message -> new MutableReference<FDate>(message));
    }

    protected void runBufferingIteratorPerformanceTest(
            final IBufferingIterator<? extends IReference<FDate>> responseQueue,
            final IBufferingIterator<? extends IReference<FDate>> requestQueue, final Object synchronizeRequest,
            final Object synchronizeResponse, final Function<FDate, IReference<FDate>> referenceFactory)
            throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = maybeSynchronize(
                new BufferingIteratorSynchronousWriter<FDate>(responseQueue) {
                    @Override
                    protected IReference<FDate> newEmptyReference() {
                        return referenceFactory.apply(null);
                    }

                    @Override
                    protected IReference<FDate> newReference(final FDate message) {
                        return referenceFactory.apply(message);
                    }
                }, synchronizeResponse);
        final ISynchronousReader<FDate> requestReader = maybeSynchronize(
                new BufferingIteratorSynchronousReader<FDate>(requestQueue), synchronizeRequest);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runBufferingIteratorPerformanceTest", 1);
        executor.execute(new ServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = maybeSynchronize(
                new BufferingIteratorSynchronousWriter<FDate>(requestQueue) {
                    @Override
                    protected IReference<FDate> newEmptyReference() {
                        return referenceFactory.apply(null);
                    }

                    @Override
                    protected IReference<FDate> newReference(final FDate message) {
                        return referenceFactory.apply(message);
                    }
                }, synchronizeRequest);
        final ISynchronousReader<FDate> responseReader = maybeSynchronize(
                new BufferingIteratorSynchronousReader<FDate>(responseQueue), synchronizeResponse);
        new ClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
