package de.invesdwin.context.integration.channel.rpc.base.server.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class CopyBufferServiceSynchronousCommand implements IServiceSynchronousCommand<IByteBufferProvider> {

    protected int service;
    protected int method;
    protected int sequence;
    protected final IByteBuffer messageHolder = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

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
        return messageHolder.sliceTo(messageSize);
    }

    public void setMessage(final IByteBufferProvider message) throws IOException {
        messageSize = message.getBuffer(messageHolder);
    }

    @Override
    public void close() {
        messageSize = 0;
    }

    public void copy(final IServiceSynchronousCommand<IByteBufferProvider> command) throws IOException {
        setService(command.getService());
        setMethod(command.getMethod());
        setSequence(command.getSequence());
        setMessage(command.getMessage());
    }

}
