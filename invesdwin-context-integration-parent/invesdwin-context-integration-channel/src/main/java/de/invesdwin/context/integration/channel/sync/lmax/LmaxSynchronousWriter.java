package de.invesdwin.context.integration.channel.sync.lmax;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class LmaxSynchronousWriter<M> implements ISynchronousWriter<M> {

    private RingBuffer<IMutableReference<M>> ringBuffer;

    public LmaxSynchronousWriter(final RingBuffer<IMutableReference<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        write(null);
        ringBuffer = null;
    }

    @Override
    public void write(final M message) throws IOException {
        final long seq = ringBuffer.next(); // blocked by ringBuffer's gatingSequence
        final IMutableReference<M> event = ringBuffer.get(seq);
        event.set(message);
        ringBuffer.publish(seq);
    }

}
