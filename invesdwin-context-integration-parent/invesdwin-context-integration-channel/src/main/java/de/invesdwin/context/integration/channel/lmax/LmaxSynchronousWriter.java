package de.invesdwin.context.integration.channel.lmax;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.lmax.disruptor.RingBuffer;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.MutableSynchronousCommand;

@NotThreadSafe
public class LmaxSynchronousWriter<M> implements ISynchronousWriter<M> {

    private RingBuffer<MutableSynchronousCommand<M>> ringBuffer;

    public LmaxSynchronousWriter(final RingBuffer<MutableSynchronousCommand<M>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        write(EmptySynchronousCommand.getInstance());
        ringBuffer = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final long seq = ringBuffer.next(); // blocked by ringBuffer's gatingSequence
        final MutableSynchronousCommand<M> event = ringBuffer.get(seq);
        event.setType(type);
        event.setSequence(sequence);
        event.setMessage(message);
        ringBuffer.publish(seq);
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

}
