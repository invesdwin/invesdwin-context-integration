package de.invesdwin.context.integration.channel.sync.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class CommandSynchronousReader<M> implements ISynchronousReader<ISynchronousCommand<M>> {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final ISerde<M> messageSerde;

    private final DeserializingSynchronousCommand<M> command = new DeserializingSynchronousCommand<>();

    public CommandSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
            final ISerde<M> messageSerde) {
        this.delegate = delegate;
        this.messageSerde = messageSerde;
    }

    public ISerde<M> getMessageSerde() {
        return messageSerde;
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
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public ISynchronousCommand<M> readMessage() throws IOException {
        final IByteBuffer buffer = delegate.readMessage().asBuffer();
        final int type = buffer.getInt(SynchronousCommandSerde.TYPE_INDEX);
        final int sequence = buffer.getInt(SynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - SynchronousCommandSerde.MESSAGE_INDEX;
        command.setType(type);
        command.setSequence(sequence);
        command.setMessage(messageSerde, buffer.slice(SynchronousCommandSerde.MESSAGE_INDEX, messageLength));
        return command;
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

}
