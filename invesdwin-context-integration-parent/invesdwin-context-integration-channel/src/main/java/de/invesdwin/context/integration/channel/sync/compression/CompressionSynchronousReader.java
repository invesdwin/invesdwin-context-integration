package de.invesdwin.context.integration.channel.sync.compression;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Decompresses each message separately. Worse compression ratio because each message is isolated. Stateless regarding
 * the connection.
 */
@NotThreadSafe
public class CompressionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = CompressionSynchronousWriter.DEFAULT_COMPRESSION_FACTORY;

    private final ISynchronousReader<IByteBuffer> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer decompressedBuffer;

    public CompressionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public CompressionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final ICompressionFactory compressionFactory) {
        this.delegate = delegate;
        this.compressionFactory = compressionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decompressedBuffer = ByteBuffers.allocateExpandable();
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
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer compressedBuffer = delegate.readMessage();
        compressionFactory.decompress(compressedBuffer, decompressedBuffer);
        return decompressedBuffer;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
