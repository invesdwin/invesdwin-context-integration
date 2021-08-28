package de.invesdwin.context.integration.channel.serde;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.lang.buffer.ByteBuffers;
import de.invesdwin.util.lang.buffer.IByteBuffer;
import de.invesdwin.util.marshallers.serde.ISerde;

@NotThreadSafe
public class SerdeSynchronousWriter<M> implements ISynchronousWriter<M> {

    private final ISynchronousWriter<IByteBuffer> delegate;
    private final ISerde<M> serde;
    private final int fixedLength;
    private final IByteBuffer buffer;

    public SerdeSynchronousWriter(final ISynchronousWriter<IByteBuffer> delegate, final ISerde<M> serde,
            final Integer fixedLength) {
        this.delegate = delegate;
        this.serde = serde;
        this.fixedLength = ByteBuffers.newAllocateFixedLength(fixedLength);
        this.buffer = ByteBuffers.allocate(this.fixedLength);
    }

    public ISerde<M> getSerde() {
        return serde;
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
    public void write(final M message) throws IOException {
        final int length = serde.toBuffer(buffer, message);
        delegate.write(buffer.slice(0, length));
    }

}
