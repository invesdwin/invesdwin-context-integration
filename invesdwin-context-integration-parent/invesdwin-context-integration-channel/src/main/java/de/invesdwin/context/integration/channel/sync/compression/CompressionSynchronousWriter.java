package de.invesdwin.context.integration.channel.sync.compression;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.FastLZ4CompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Compress each message separately. Worse compression ratio because each message is isolated. Stateless regarding the
 * connection.
 */
@NotThreadSafe
public class CompressionSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = FastLZ4CompressionFactory.INSTANCE;

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decompressedBuffer;

    public CompressionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public CompressionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final ICompressionFactory compressionFactory) {
        this.delegate = delegate;
        this.compressionFactory = compressionFactory;
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
    public void write(final IByteBufferWriter message) throws IOException {
        this.decompressedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.decompressedBuffer = null;
        }
    }

    @Override
    public int writeBuffer(final IByteBuffer buffer) {
        return compressionFactory.compress(decompressedBuffer, buffer);
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            buffer = ByteBuffers.allocateExpandable();
        }
        final int length = writeBuffer(buffer);
        return buffer.slice(0, length);
    }

}
