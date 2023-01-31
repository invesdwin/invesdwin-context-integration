package de.invesdwin.context.integration.channel.sync.compression.stream;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.compression.CompressionSynchronousWriter;
import de.invesdwin.context.integration.compression.ICompressionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.DisabledByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.stream.ExpandableByteBufferOutputStream;

/**
 * Compress multiple messages as if they are from a continuous stream. Better compression ratio but it is stateful to
 * the connection. Also the compressed fragment header is larger than in an individual compression.
 */
@NotThreadSafe
public class StreamCompressionSynchronousWriter
        implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    public static final int DECOMPRESSEDLENGTH_INDEX = 0;
    public static final int DECOMPRESSEDLENGTH_SIZE = Integer.BYTES;

    public static final int PAYLOAD_INDEX = DECOMPRESSEDLENGTH_INDEX + DECOMPRESSEDLENGTH_SIZE;

    public static final ICompressionFactory DEFAULT_COMPRESSION_FACTORY = CompressionSynchronousWriter.DEFAULT_COMPRESSION_FACTORY;

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final ICompressionFactory compressionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decompressedBuffer;

    private ExpandableByteBufferOutputStream compressingStreamOut;
    private OutputStream compressingStreamIn;

    public StreamCompressionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate) {
        this(delegate, DEFAULT_COMPRESSION_FACTORY);
    }

    public StreamCompressionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
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
        compressingStreamOut = new ExpandableByteBufferOutputStream();
        compressingStreamIn = compressionFactory.newCompressor(compressingStreamOut, false);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
        if (compressingStreamOut != null) {
            compressingStreamOut.wrap(DisabledByteBuffer.INSTANCE); //prevent segmentation fault
            compressingStreamOut = null;
        }
        if (compressingStreamIn != null) {
            compressingStreamIn.close();
            compressingStreamIn = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.decompressedBuffer = message.asBuffer();
        delegate.write(this);
    }

    @Override
    public boolean writeFinished() throws IOException {
        if (delegate.writeFinished()) {
            this.decompressedBuffer = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        compressingStreamOut.wrap(dst.sliceFrom(PAYLOAD_INDEX));
        decompressedBuffer.getBytes(0, compressingStreamIn);
        compressingStreamIn.flush();
        final int compressedLength = compressingStreamOut.position();
        dst.putInt(DECOMPRESSEDLENGTH_INDEX, decompressedBuffer.capacity());
        return PAYLOAD_INDEX + compressedLength;
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        if (buffer == null) {
            buffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(buffer);
        return buffer.slice(0, length);
    }

}
