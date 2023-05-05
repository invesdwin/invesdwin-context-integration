package de.invesdwin.context.integration.channel.rpc.server.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class SerializingServiceSynchronousCommand<M> implements IServiceSynchronousCommand<IByteBufferProvider> {

    protected int service;
    protected int method;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
    protected M message;

    @Override
    public int getService() {
        return service;
    }

    public void setService(final int service) {
        this.service = service;
    }

    @Override
    public int getMethod() {
        return method;
    }

    public void setMethod(final int method) {
        this.method = method;
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public void setSequence(final int sequence) {
        this.sequence = sequence;
    }

    @Override
    public IByteBufferProvider getMessage() {
        throw new UnsupportedOperationException();
    }

    public void setMessage(final ISerde<M> messageSerde, final M message) {
        this.messageSerde = messageSerde;
        this.message = message;
    }

    @Override
    public int messageToBuffer(final ISerde<IByteBufferProvider> messageSerde, final IByteBuffer buffer) {
        return this.messageSerde.toBuffer(buffer, message);
    }

    @Override
    public void close() throws IOException {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
