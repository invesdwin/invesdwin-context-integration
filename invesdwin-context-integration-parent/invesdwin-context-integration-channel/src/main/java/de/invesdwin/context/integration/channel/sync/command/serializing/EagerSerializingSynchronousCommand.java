package de.invesdwin.context.integration.channel.sync.command.serializing;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class EagerSerializingSynchronousCommand<M> implements ISerializingSynchronousCommand<M> {

    protected int type;
    protected int sequence;
    //direct buffer for outgoing information into a transport
    protected final IByteBuffer messageHolder = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

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
        messageSize = messageSerde.toBuffer(messageHolder, message);
    }

    @Override
    public int messageToBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        buffer.putBytesTo(0, messageHolder, messageSize);
        return messageSize;
    }

    @Override
    public void close() throws IOException {
        messageSize = 0;
    }

}
