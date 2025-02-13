package de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class EagerSerializingServiceSynchronousCommand<M> implements ISerializingServiceSynchronousCommand<M> {

    //direct buffer for outgoing information into a transport
    protected final IByteBuffer buffer = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

    @Override
    public int getService() {
        return buffer.getInt(ServiceSynchronousCommandSerde.SERVICE_INDEX);
    }

    @Override
    public void setService(final int service) {
        buffer.putInt(ServiceSynchronousCommandSerde.SERVICE_INDEX, service);
    }

    @Override
    public int getMethod() {
        return buffer.getInt(ServiceSynchronousCommandSerde.METHOD_INDEX);
    }

    @Override
    public void setMethod(final int method) {
        buffer.putInt(ServiceSynchronousCommandSerde.METHOD_INDEX, method);
    }

    @Override
    public int getSequence() {
        return buffer.getInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX);
    }

    @Override
    public void setSequence(final int sequence) {
        buffer.putInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX, sequence);
    }

    @Override
    public IByteBufferProvider getMessage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final M message) {
        messageSize = messageSerde.toBuffer(buffer.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX), message);
    }

    @Override
    public void setMessageBuffer(final ICloseableByteBufferProvider messageBuffer) {
        try {
            messageSize = ByteBufferProviderSerde.GET
                    .toBuffer(buffer.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX), messageBuffer);
        } finally {
            messageBuffer.close();
        }
    }

    @Override
    public int toBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        final int length = ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageSize;
        buffer.putBytesTo(0, this.buffer, length);
        return length;
    }

    @Override
    public void close() {
        messageSize = 0;
    }

    public IByteBuffer asBuffer() {
        return buffer.sliceTo(ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageSize);
    }

}
