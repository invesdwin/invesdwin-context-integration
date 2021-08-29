package de.invesdwin.context.integration.channel.serde;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.ByteBuffers;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class SerdeSynchronousWriter<M> implements ISynchronousWriter<M>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final ISerde<M> serde;
    private final int fixedLength;
    private IByteBuffer buffer;
    private M message;

    public SerdeSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate, final ISerde<M> serde,
            final Integer fixedLength) {
        this.delegate = delegate;
        this.serde = serde;
        this.fixedLength = ByteBuffers.newAllocateFixedLength(fixedLength);
    }

    public ISerde<M> getSerde() {
        return serde;
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
    }

    @Override
    public void write(final M message) throws IOException {
        this.message = message;
        delegate.write(this);
        this.message = null;
    }

    @Override
    public int write(final IByteBuffer buffer) {
        return serde.toBuffer(buffer, message);
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
