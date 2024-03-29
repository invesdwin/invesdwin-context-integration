package de.invesdwin.context.integration.channel.sync.command.serializing;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.command.SynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class LazySerializingSynchronousCommand<M> implements ISerializingSynchronousCommand<M> {

    protected int type;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
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
    public IByteBufferProvider getMessage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final M message) {
        this.messageSerde = messageSerde;
        this.message = message;
    }

    @Override
    public int toBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, getType());
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, getSequence());
        final int messageLength = this.messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                message);
        return SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
    }

    @Override
    public void close() throws IOException {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
