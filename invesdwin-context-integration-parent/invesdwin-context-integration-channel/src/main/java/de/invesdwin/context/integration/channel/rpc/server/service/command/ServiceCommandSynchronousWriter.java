package de.invesdwin.context.integration.channel.rpc.server.service.command;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ServiceCommandSynchronousWriter<M>
        implements ISynchronousWriter<IServiceSynchronousCommand<M>>, IByteBufferProvider {

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final ISerde<M> messageSerde;
    private final Integer maxMessageLength;
    private final int fixedLength;
    private IByteBuffer buffer;
    private IServiceSynchronousCommand<M> message;

    @SuppressWarnings("unchecked")
    public ServiceCommandSynchronousWriter(final ISynchronousWriter<? extends IByteBufferProvider> delegate,
            final ISerde<M> messageSerde, final Integer maxMessageLength) {
        this.delegate = (ISynchronousWriter<IByteBufferProvider>) delegate;
        this.messageSerde = messageSerde;
        this.maxMessageLength = maxMessageLength;
        this.fixedLength = ServiceSynchronousCommandSerde.newFixedLength(maxMessageLength);
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

    public ISynchronousWriter<IByteBufferProvider> getDelegate() {
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
    public boolean writeReady() throws IOException {
        return delegate.writeReady();
    }

    @Override
    public void write(final IServiceSynchronousCommand<M> message) throws IOException {
        this.message = message;
        delegate.write(this);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (delegate.writeFlushed()) {
            this.message = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        dst.putInt(ServiceSynchronousCommandSerde.SERVICE_INDEX, message.getService());
        dst.putInt(ServiceSynchronousCommandSerde.METHOD_INDEX, message.getMethod());
        dst.putInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX, message.getSequence());
        final int messageLength = message.messageToBuffer(messageSerde,
                dst.sliceFrom(ServiceSynchronousCommandSerde.MESSAGE_INDEX));
        final int length = ServiceSynchronousCommandSerde.MESSAGE_INDEX + messageLength;
        return length;
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            //needs to be expandable so that FragmentSynchronousWriter can work properly
            buffer = ByteBuffers.allocateExpandable(fixedLength);
        }
        final int length = getBuffer(buffer);
        return buffer.slice(0, length);
    }

}
