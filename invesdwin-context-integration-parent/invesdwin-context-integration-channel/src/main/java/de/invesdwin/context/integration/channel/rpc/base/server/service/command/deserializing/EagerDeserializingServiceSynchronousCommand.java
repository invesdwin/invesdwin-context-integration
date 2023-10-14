package de.invesdwin.context.integration.channel.rpc.base.server.service.command.deserializing;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class EagerDeserializingServiceSynchronousCommand<M> implements IDeserializingServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
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
    public M getMessage() {
        return message;
    }

    @Override
    public void setMessage(final ISerde<M> messageSerde, final IByteBuffer message) {
        this.message = messageSerde.fromBuffer(message);
    }

    @Override
    public void close() {
        message = null;
    }

}
