package de.invesdwin.context.integration.channel.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.lang.buffer.IByteBuffer;
import de.invesdwin.util.marshallers.serde.ISerde;

@NotThreadSafe
public class CommandSynchronousReader<M> implements ISynchronousReader<ISynchronousCommand<M>> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final ISerde<M> messageSerde;

    private final MutableSynchronousCommand<M> command = new MutableSynchronousCommand<>();

    public CommandSynchronousReader(final ISynchronousReader<IByteBuffer> delegate, final ISerde<M> messageSerde) {
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
        final IByteBuffer buffer = delegate.readMessage();
        final int type = buffer.getInt(SynchronousCommandSerde.TYPE_INDEX);
        final int sequence = buffer.getInt(SynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - SynchronousCommandSerde.MESSAGE_INDEX;
        final M message = messageSerde.fromBuffer(buffer.slice(SynchronousCommandSerde.MESSAGE_INDEX, messageLength),
                messageLength);
        command.setType(type);
        command.setSequence(sequence);
        command.setMessage(message);
        return command;
    }

}
