package de.invesdwin.context.integration.channel.rpc.server.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing.IDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing.LazyDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ServiceCommandSynchronousReader<M> implements ISynchronousReader<IServiceSynchronousCommand<M>> {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final ISerde<M> messageSerde;

    private final IDeserializingServiceSynchronousCommand<M> command = new LazyDeserializingServiceSynchronousCommand<>();

    public ServiceCommandSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
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
    public IServiceSynchronousCommand<M> readMessage() throws IOException {
        final IByteBuffer buffer = delegate.readMessage().asBuffer();
        final int service = buffer.getInt(ServiceSynchronousCommandSerde.SERVICE_INDEX);
        final int method = buffer.getInt(ServiceSynchronousCommandSerde.METHOD_INDEX);
        final int sequence = buffer.getInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - ServiceSynchronousCommandSerde.MESSAGE_INDEX;
        command.setService(service);
        command.setMethod(method);
        command.setSequence(sequence);
        command.setMessage(messageSerde, buffer.slice(ServiceSynchronousCommandSerde.MESSAGE_INDEX, messageLength));
        return command;
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

}
