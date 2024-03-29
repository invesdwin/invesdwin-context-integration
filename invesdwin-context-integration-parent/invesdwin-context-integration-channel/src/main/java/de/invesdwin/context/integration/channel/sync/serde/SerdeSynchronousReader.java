package de.invesdwin.context.integration.channel.sync.serde;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class SerdeSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final ISerde<M> serde;

    public SerdeSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate, final ISerde<M> serde) {
        this.delegate = delegate;
        this.serde = serde;
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
    public M readMessage() throws IOException {
        final IByteBuffer message = delegate.readMessage().asBuffer();
        return serde.fromBuffer(message);
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

}
