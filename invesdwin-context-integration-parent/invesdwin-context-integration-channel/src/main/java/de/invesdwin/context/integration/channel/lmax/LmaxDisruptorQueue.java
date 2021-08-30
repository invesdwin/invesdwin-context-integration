package de.invesdwin.context.integration.channel.lmax;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

/**
 * Extracted from:
 * https://github.com/xinglang/disruptorqueue/blob/master/disruptorqueue/src/main/java/disruptorqueue/SingleConsumerDisruptorQueue.java
 * 
 * Only a single consumer is supported.
 */
@ThreadSafe
public class LmaxDisruptorQueue<T> extends AbstractQueue<T> implements BlockingQueue<T> {
    /**
     * An event holder.
     */
    private static class Event<T> {
        private T item;

        public T getValue() {
            final T t = item;
            item = null;
            return t;
        }

        public T readValue() {
            return item;
        }

        public void setValue(final T event) {
            this.item = event;
        }
    }

    /**
     * Event factory to create event holder instance.
     */
    private static class Factory<T> implements EventFactory<Event<T>> {

        @Override
        public Event<T> newInstance() {
            return new Event<T>();
        }

    }

    private final RingBuffer<Event<T>> ringBuffer;
    private final Sequence consumedSeq;
    private final SequenceBarrier barrier;
    private long knownPublishedSeq;

    /**
     * Construct a blocking queue based on disruptor and allows multiple producers.
     * 
     * @param bufferSize
     */
    public LmaxDisruptorQueue(final int bufferSize) {
        this(bufferSize, false);
    }

    /**
     * Construct a blocking queue based on disruptor.
     * 
     * @param bufferSize
     *            ring buffer size
     * @param singleProducer
     *            whether only single thread produce events.
     */
    public LmaxDisruptorQueue(final int bufferSize, final boolean singleProducer) {
        if (singleProducer) {
            ringBuffer = RingBuffer.createSingleProducer(new Factory<T>(), normalizeBufferSize(bufferSize));
        } else {
            ringBuffer = RingBuffer.createMultiProducer(new Factory<T>(), normalizeBufferSize(bufferSize));
        }

        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        barrier = ringBuffer.newBarrier();

        final long cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;
    }

    @Override
    public int drainTo(final Collection<? super T> collection) {
        return drainTo(collection, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(final Collection<? super T> collection, final int maxElements) {
        long pos = consumedSeq.get() + 1;
        if (pos + maxElements - 1 > knownPublishedSeq) {
            updatePublishedSequence();
        }
        int c = 0;
        try {
            while (pos <= knownPublishedSeq && c <= maxElements) {
                final Event<T> eventHolder = ringBuffer.get(pos);
                collection.add(eventHolder.getValue());
                c++;
                pos++;
            }
        } finally {
            if (c > 0) {
                consumedSeq.addAndGet(c);
            }
        }
        return c;
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    private int normalizeBufferSize(final int bufferSize) {
        if (bufferSize <= 0) {
            return 8192;
        }
        int ringBufferSize = 2;
        while (ringBufferSize < bufferSize) {
            ringBufferSize *= 2;
        }
        return ringBufferSize;
    }

    @Override
    public boolean offer(final T e) {
        final long seq;
        try {
            seq = ringBuffer.tryNext();
        } catch (final InsufficientCapacityException e1) {
            return false;
        }
        publish(e, seq);
        return true;
    }

    @Override
    public boolean offer(final T e, final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public T peek() {
        final long l = consumedSeq.get() + 1;
        if (l > knownPublishedSeq) {
            updatePublishedSequence();
        }
        if (l <= knownPublishedSeq) {
            final Event<T> eventHolder = ringBuffer.get(l);
            return eventHolder.readValue();
        }
        return null;
    }

    @Override
    public T poll() {
        final long l = consumedSeq.get() + 1;
        if (l > knownPublishedSeq) {
            updatePublishedSequence();
        }
        if (l <= knownPublishedSeq) {
            final Event<T> eventHolder = ringBuffer.get(l);
            final T t = eventHolder.getValue();
            consumedSeq.incrementAndGet();
            return t;
        }
        return null;
    }

    @Override
    public T poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    private void publish(final T e, final long seq) {
        final Event<T> holder = ringBuffer.get(seq);
        holder.setValue(e);
        ringBuffer.publish(seq);
    }

    @Override
    public void put(final T e) throws InterruptedException {
        final long seq = ringBuffer.next();
        publish(e, seq);
    }

    @Override
    public int remainingCapacity() {
        return ringBuffer.getBufferSize() - size();
    }

    @Override
    public int size() {
        return (int) (ringBuffer.getCursor() - consumedSeq.get());
    }

    @Override
    public T take() throws InterruptedException {
        final long l = consumedSeq.get() + 1;
        while (knownPublishedSeq < l) {
            try {
                knownPublishedSeq = barrier.waitFor(l);
            } catch (final AlertException e) {
                throw new IllegalStateException(e);
            } catch (final TimeoutException e) {
                throw new IllegalStateException(e);
            }
        }
        final Event<T> eventHolder = ringBuffer.get(l);
        final T t = eventHolder.getValue();
        consumedSeq.incrementAndGet();
        return t;
    }

    @Override
    public String toString() {
        return "Cursor: " + ringBuffer.getCursor() + ", Consumerd :" + consumedSeq.get();
    }

    @SuppressWarnings("deprecation")
    private void updatePublishedSequence() {
        final long c = ringBuffer.getCursor();
        if (c >= knownPublishedSeq + 1) {
            long pos = c;
            for (long sequence = knownPublishedSeq + 1; sequence <= c; sequence++) {
                if (!ringBuffer.isPublished(sequence)) {
                    pos = sequence - 1;
                    break;
                }
            }
            knownPublishedSeq = pos;
        }
    }
}
