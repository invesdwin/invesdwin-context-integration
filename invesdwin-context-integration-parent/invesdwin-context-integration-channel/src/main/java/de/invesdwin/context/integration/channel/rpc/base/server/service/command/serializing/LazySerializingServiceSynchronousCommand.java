package de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class LazySerializingServiceSynchronousCommand<M> implements ISerializingServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
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
    public int toBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        buffer.putInt(ServiceSynchronousCommandSerde.SERVICE_INDEX, getService());
        buffer.putInt(ServiceSynchronousCommandSerde.METHOD_INDEX, getMethod());
        buffer.putInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX, getSequence());
        final int messageLength = this.messageSerde
                .toBuffer(buffer.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX), message);
        return ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageLength;
    }

    @Override
    public void close() {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
