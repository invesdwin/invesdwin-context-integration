package de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@NotThreadSafe
public class LazySerializingServiceSynchronousCommand<M> implements ISerializingServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    protected IByteBufferProvider messageBuffer;
    protected ICloseableByteBufferProvider closeableMessageBuffer;
    protected ISerde<M> messageSerde;
    protected M message;

    @Override
    public int getService() {
        return service;
    }

    @Override
    public void setService(final int service) {
        this.service = service;
    }

    @Override
    public int getMethod() {
        return method;
    }

    @Override
    public void setMethod(final int method) {
        this.method = method;
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
    public void setMessageBuffer(final IByteBufferProvider messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

    @Override
    public void setCloseableMessageBuffer(final ICloseableByteBufferProvider messageBuffer) {
        setMessageBuffer(messageBuffer);
        this.closeableMessageBuffer = messageBuffer;
    }

    @Override
    public int toBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        assert hasMessage();
        buffer.putInt(ServiceSynchronousCommandSerde.SERVICE_INDEX, getService());
        buffer.putInt(ServiceSynchronousCommandSerde.METHOD_INDEX, getMethod());
        buffer.putInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX, getSequence());
        if (messageBuffer != null) {
            final int messageLength = ByteBufferProviderSerde.GET
                    .toBuffer(buffer.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX), messageBuffer);
            return ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        } else {
            final int messageLength = this.messageSerde
                    .toBuffer(buffer.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX), message);
            return ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        }
    }

    @Override
    public boolean hasMessage() {
        return messageSerde != null || messageBuffer != null;
    }

    @Override
    public void close() {
        if (messageBuffer != null) {
            if (closeableMessageBuffer != null) {
                closeableMessageBuffer.close();
                closeableMessageBuffer = null;
            }
            messageBuffer = null;
        } else {
            messageSerde = null;
            message = null; //free memory
        }
    }

}
