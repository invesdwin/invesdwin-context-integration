package de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class LazyDeserializingServiceSynchronousCommand<M> implements IDeserializingServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
    protected IByteBuffer message;

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
    public M getMessage() {
        return messageSerde.fromBuffer(message);
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final IByteBuffer message) {
        this.messageSerde = messageSerde;
        this.message = message;
    }

    @Override
    public void close() {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
