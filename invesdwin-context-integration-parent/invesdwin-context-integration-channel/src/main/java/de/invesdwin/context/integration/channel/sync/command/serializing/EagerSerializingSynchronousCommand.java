package de.invesdwin.context.integration.channel.sync.command.serializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.command.SynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class EagerSerializingSynchronousCommand<M> implements ISerializingSynchronousCommand<M> {

    //direct buffer for outgoing information into a transport
    protected final IByteBuffer buffer = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

    @Override
    public int getType() {
        return buffer.getInt(SynchronousCommandSerde.TYPE_INDEX);
    }

    @Override
    public void setType(final int type) {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, type);
    }

    @Override
    public int getSequence() {
        return buffer.getInt(SynchronousCommandSerde.SEQUENCE_INDEX);
    }

    @Override
    public void setSequence(final int sequence) {
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, sequence);
    }

    @Override
    public IByteBufferProvider getMessage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final M message) {
        messageSize = messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX), message);
    }

    @Override
    public int toBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageSize;
        buffer.putBytesTo(0, buffer, length);
        return length;
    }

    @Override
    public void close() {
        messageSize = 0;
    }

    public IByteBuffer asBuffer() {
        return buffer.sliceTo(SynchronousCommandSerde.MESSAGE_INDEX + messageSize);
    }

}
