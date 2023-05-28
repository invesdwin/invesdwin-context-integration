package de.invesdwin.context.integration.channel.sync.command.deserializing;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class EagerDeserializingSynchronousCommand<M> implements IDeserializingSynchronousCommand<M> {

    protected int type;
    protected int sequence;
    protected M message;

    @Override
    public int getType() {
        return type;
    }

    @Override
    public void setType(final int type) {
        this.type = type;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    @Override
    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    @Override
    public M getMessage() {
        return message;
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final IByteBuffer message) {
        this.message = messageSerde.fromBuffer(message);
    }

    @Override
    public void close() throws IOException {
        message = null;
    }

}
