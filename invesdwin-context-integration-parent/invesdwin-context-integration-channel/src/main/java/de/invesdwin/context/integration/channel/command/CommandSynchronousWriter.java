package de.invesdwin.context.integration.channel.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.IByteBuffer;

@NotThreadSafe
public class CommandSynchronousWriter<M> implements ISynchronousWriter<ISynchronousCommand<M>> {

    private final ISynchronousWriter<IByteBuffer> delegate;
    private final ISerde<M> messageSerde;
    private final Integer maxMessageLength;
    private final int fixedLength;
    private final IByteBuffer buffer;

    public CommandSynchronousWriter(final ISynchronousWriter<IByteBuffer> delegate, final ISerde<M> messageSerde,
            final Integer maxMessageLength) {
        this.delegate = delegate;
        this.messageSerde = messageSerde;
        this.maxMessageLength = maxMessageLength;
        this.fixedLength = SynchronousCommandSerde.newFixedLength(maxMessageLength);
        this.buffer = ByteBuffers.allocate(this.fixedLength);
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

    public ISynchronousWriter<IByteBuffer> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, message.getType());
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, message.getSequence());
        final int messageLength = messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                message.getMessage());
        final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        delegate.write(buffer.slice(0, length));
    }

}
