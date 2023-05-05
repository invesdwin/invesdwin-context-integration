package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class SerdeMutableSynchronousCommand<M> implements ISynchronousCommand<M> {

    protected int type;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
    protected IByteBuffer message;

    @Override
    public int getType() {
        return type;
    }

    public void setType(final int method) {
        this.type = method;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    @Override
    public M getMessage() {
        return messageSerde.fromBuffer(message);
    }

    public void setMessage(final ISerde<M> messageSerde, final IByteBuffer message) {
        this.messageSerde = messageSerde;
        this.message = message;
    }

    @Override
    public void close() throws IOException {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
