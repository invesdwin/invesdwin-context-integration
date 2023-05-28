package de.invesdwin.context.integration.channel.rpc.server.service.command.serializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class EagerSerializingServiceSynchronousCommand<M> implements ISerializingServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    //direct buffer for outgoing information into a transport
    protected final IByteBuffer messageHolder = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

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
        messageSize = messageSerde.toBuffer(messageHolder, message);
    }

    @Override
    public int messageToBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        buffer.putBytesTo(0, messageHolder, messageSize);
        return messageSize;
    }

    @Override
    public void close() {
        messageSize = 0;
    }

}
