package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class CommandSynchronousWriter<M> implements ISynchronousWriter<ISynchronousCommand<M>>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final ISerde<M> messageSerde;
    private final Integer maxMessageLength;
    private final int fixedLength;
    private IByteBuffer buffer;
    private ISynchronousCommand<M> message;

    @SuppressWarnings("unchecked")
    public CommandSynchronousWriter(final ISynchronousWriter<? extends IByteBufferWriter> delegate,
            final ISerde<M> messageSerde, final Integer maxMessageLength) {
        this.delegate = (ISynchronousWriter<IByteBufferWriter>) delegate;
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
        try {
            delegate.write(this);
        } finally {
            this.message = null;
        }
    }

    @Override
    public int writeBuffer(final IByteBuffer buffer) {
        buffer.putInt(SynchronousCommandSerde.TYPE_INDEX, message.getType());
        buffer.putInt(SynchronousCommandSerde.SEQUENCE_INDEX, message.getSequence());
        final int messageLength = messageSerde.toBuffer(buffer.sliceFrom(SynchronousCommandSerde.MESSAGE_INDEX),
                message.getMessage());
        final int length = SynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        return length;
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            //needs to be expandable so that FragmentSynchronousWriter can work properly
            buffer = ByteBuffers.allocateExpandable(fixedLength);
        }
        final int length = writeBuffer(buffer);
        return buffer.slice(0, length);
    }

}
