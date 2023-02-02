package de.invesdwin.context.integration.channel.sync.compression;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.context.integration.compression.lz4.FastLZ4CompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Compress each message separately. Worse compression ratio because each message is isolated. Stateless regarding the
 * connection.
 */
@NotThreadSafe
public class CompressionSynchronousWriter implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = FastLZ4CompressionFactory.INSTANCE;

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decompressedBuffer;

    public CompressionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public CompressionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final ICompressionFactory compressionFactory) {
        this.delegate = delegate;
        this.compressionFactory = compressionFactory;
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
    public void write(final IByteBufferProvider message) throws IOException {
        this.decompressedBuffer = message.asBuffer();
        delegate.write(this);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (delegate.writeFlushed()) {
            this.decompressedBuffer = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        return compressionFactory.compress(decompressedBuffer, dst);
    }

    @Override
    public IByteBuffer asBuffer() {
        if (buffer == null) {
            buffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(buffer);
        return buffer.slice(0, length);
    }

}
