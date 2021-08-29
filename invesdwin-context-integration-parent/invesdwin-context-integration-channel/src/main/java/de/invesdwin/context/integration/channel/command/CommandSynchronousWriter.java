package de.invesdwin.context.integration.channel.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class CommandSynchronousWriter<M> implements ISynchronousWriter<ISynchronousCommand<M>>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final ISerde<M> messageSerde;
    private final Integer maxMessageLength;
    private final int fixedLength;
    private IByteBuffer buffer;
    private ISynchronousCommand<M> message;

    public CommandSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate, final ISerde<M> messageSerde,
            final Integer maxMessageLength) {
        this.delegate = delegate;
        this.messageSerde = messageSerde;
        this.maxMessageLength = maxMessageLength;
        this.fixedLength = SynchronousCommandSerde.newFixedLength(maxMessageLength);
    }

    public ISerde<M> getMessageSerde() {
        return messageSerde;
    }

    public Integer getMaxMessageLength() {
        return maxMessageLength;
    }

    public int getFixedLength() {
        return fixedLength;
    }

    public ISynchronousWriter<IByteBufferWriter> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        this.message = message;
        delegate.write(this);
        this.message = null;
    }

    @Override
    public int write(final IByteBuffer buffer) {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, message.getType());
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, message.getSequence());
        final int messageLength = messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                message.getMessage());
        final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        return length;
    }

    @Override
    public IByteBuffer asByteBuffer() {
        if (buffer == null) {
            buffer = ByteBuffers.allocate(this.fixedLength);
        }
        final int length = write(buffer);
        return buffer.slice(0, length);
    }

}
