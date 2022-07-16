package de.invesdwin.context.integration.channel.sync.compression.stream;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.streams.compressor.ICompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.stream.ByteBufferInputStream;

/**
 * Decompress multiple messages as if they come from a continuous stream. Better compression ratio but it is stateful to
 * the connection. Also the compressed fragment header is larger than in an individual compression.
 */
@NotThreadSafe
public class StreamCompressionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = StreamCompressionSynchronousWriter.DEFAULT_COMPRESSION_FACTORY;

    private final ISynchronousReader<IByteBuffer> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer decompressedBuffer;
    private ByteBufferInputStream decompressingStreamIn;
    private InputStream decompressingStreamOut;

    public StreamCompressionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public StreamCompressionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final ICompressionFactory compressionFactory) {
        this.delegate = delegate;
        this.compressionFactory = compressionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decompressedBuffer = ByteBuffers.allocateExpandable();
        decompressingStreamIn = new ByteBufferInputStream();
        decompressingStreamOut = compressionFactory.newDecompressor(decompressingStreamIn);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decompressedBuffer = null;
        decompressingStreamIn = null;
        if (decompressingStreamOut != null) {
            decompressingStreamOut.close();
            decompressingStreamOut = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer compressedBuffer = delegate.readMessage();
        final int length = compressedBuffer.getInt(StreamCompressionSynchronousWriter.DECOMPRESSEDLENGTH_INDEX);
        final IByteBuffer payloadBuffer = compressedBuffer.sliceFrom(StreamCompressionSynchronousWriter.PAYLOAD_INDEX);
        decompressingStreamIn.wrap(payloadBuffer);
        decompressedBuffer.putBytesTo(0, decompressingStreamOut, length);
        return decompressedBuffer;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
