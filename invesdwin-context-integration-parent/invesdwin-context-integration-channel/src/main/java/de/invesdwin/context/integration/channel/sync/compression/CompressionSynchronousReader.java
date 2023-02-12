package de.invesdwin.context.integration.channel.sync.compression;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Decompresses each message separately. Worse compression ratio because each message is isolated. Stateless regarding
 * the connection.
 */
@NotThreadSafe
public class CompressionSynchronousReader implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = CompressionSynchronousWriter.DEFAULT_COMPRESSION_FACTORY;

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer decompressedBuffer;

    public CompressionSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public CompressionSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
            final ICompressionFactory compressionFactory) {
        this.delegate = delegate;
        this.compressionFactory = compressionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decompressedBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        if (decompressedBuffer == null) {
            decompressedBuffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(decompressedBuffer);
        return decompressedBuffer.sliceTo(length);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final IByteBuffer compressedBuffer = delegate.readMessage().asBuffer();
        final int decompressedLength = compressionFactory.decompress(compressedBuffer, dst);
        return decompressedLength;
    }

}
