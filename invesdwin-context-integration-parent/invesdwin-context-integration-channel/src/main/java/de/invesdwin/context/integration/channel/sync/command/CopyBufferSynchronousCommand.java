package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class CopyBufferSynchronousCommand implements ISynchronousCommand<IByteBufferProvider> {

    protected int type;
    protected int sequence;
    protected final IByteBuffer messageHolder = ByteBuffers.allocateDirectExpandable();
    protected int messageSize;

    @Override
    public int getType() {
        return type;
    }

    public void setType(final int type) {
        this.type = type;
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
    public void close() throws IOException {
        messageSize = 0;
    }

    public void copy(final ISynchronousCommand<IByteBufferProvider> command) throws IOException {
        setType(command.getType());
        setSequence(command.getSequence());
        setMessage(command.getMessage());
    }

}
