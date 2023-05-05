package de.invesdwin.context.integration.channel.rpc.server.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.NullSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class DeserializingServiceSynchronousCommand<M> implements IServiceSynchronousCommand<M> {

    protected int service;
    protected int method;
    protected int sequence;
    protected ISerde<M> messageSerde = NullSerde.get();
    protected IByteBuffer message;

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
    public M getMessage() {
        return messageSerde.fromBuffer(message);
    }

    public void setMessage(final ISerde<M> messageSerde, final IByteBuffer message) {
        this.messageSerde = messageSerde;
        this.message = message;
    }

    @Override
    public void close() throws IOException {
        messageSerde = NullSerde.get();
        message = null; //free memory
    }

}
