package de.invesdwin.context.integration.channel.lmax;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class LmaxSynchronousReader<M> implements ISynchronousReader<M> {

    private RingBuffer<IMutableReference<M>> ringBuffer;
    private EventPoller<IMutableReference<M>> eventPoller;
    private ImmutableReference<M> polledValue;

    private final EventPoller.Handler<IMutableReference<M>> pollerHandler = (event, sequence, endOfBatch) -> {
        polledValue = ImmutableReference.of(event.get());
        event.set(null); //free memory
        return false;
    };

    public LmaxSynchronousReader(final RingBuffer<IMutableReference<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
        this.eventPoller = ringBuffer.newPoller();
        ringBuffer.addGatingSequences(eventPoller.getSequence());
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        ringBuffer = null;
        eventPoller = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (polledValue != null) {
            return true;
        }
        try {
            eventPoller.poll(pollerHandler);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return polledValue != null;
    }

    @Override
    public M readMessage() throws IOException {
        final ImmutableReference<M> holder = getPolledMessage();
        final M message = holder.get();
        if (message == null) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ImmutableReference<M> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableReference<M> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final EventPoller.PollState poll = eventPoller.poll(pollerHandler);
            if (poll == EventPoller.PollState.PROCESSING) {
                final ImmutableReference<M> value = polledValue;
                polledValue = null;
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
