package de.invesdwin.context.integration.channel.sync.lmax;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.MutableReference;
import de.invesdwin.util.error.FastEOFException;

@NotThreadSafe
public class LmaxSynchronousReader<M> implements ISynchronousReader<M> {

    private RingBuffer<IMutableReference<M>> ringBuffer;
    private EventPoller<IMutableReference<M>> eventPoller;
    private final IMutableReference<M> polledValueHolder = new MutableReference<>();
    private IMutableReference<M> polledValue;

    private final EventPoller.Handler<IMutableReference<M>> pollerHandler = (event, sequence, endOfBatch) -> {
        polledValueHolder.set(event.get());
        polledValue = polledValueHolder;
        event.set(null); //free memory
        return false;
    };

    public LmaxSynchronousReader(final RingBuffer<IMutableReference<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
        this.eventPoller = ringBuffer.newPoller();
        ringBuffer.addGatingSequences(eventPoller.getSequence());
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
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
        return polledValue != null;
    }

    @Override
    public M readMessage() throws IOException {
        final IMutableReference<M> holder = getPolledMessage();
        final M message = holder.getAndSet(null);
        if (message == null) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    private IMutableReference<M> getPolledMessage() throws IOException {
        if (polledValue != null) {
            final IMutableReference<M> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            final EventPoller.PollState poll = eventPoller.poll(pollerHandler);
            if (poll == EventPoller.PollState.PROCESSING) {
                final IMutableReference<M> value = polledValue;
                polledValue = null;
                return value;
            } else {
                return null;
            }
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

}
