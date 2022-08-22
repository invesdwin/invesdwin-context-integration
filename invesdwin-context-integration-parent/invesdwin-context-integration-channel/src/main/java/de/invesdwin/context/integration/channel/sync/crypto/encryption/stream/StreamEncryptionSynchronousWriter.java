package de.invesdwin.context.integration.channel.sync.crypto.encryption.stream;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.DisabledByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.stream.ExpandableByteBufferOutputStream;

/**
 * Encrypts multiple messages as if they are from a continuous stream. It is stateful to the connection and more
 * efficient due to object reuse.
 */
@NotThreadSafe
public class StreamEncryptionSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    public static final int DECRYPTEDLENGTH_INDEX = 0;
    public static final int DECRYPTEDLENGTH_SIZE = Integer.BYTES;

    public static final int PAYLOAD_INDEX = DECRYPTEDLENGTH_INDEX + DECRYPTEDLENGTH_SIZE;

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decryptedBuffer;

    private ExpandableByteBufferOutputStream encryptingStreamOut;
    private OutputStream encryptingStreamIn;

    public StreamEncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final IEncryptionFactory encryptionFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
    }

    public ISynchronousWriter<IByteBufferWriter> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        encryptingStreamOut = new ExpandableByteBufferOutputStream();
        encryptingStreamIn = encryptionFactory.newStreamingEncryptor(encryptingStreamOut);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
        if (encryptingStreamOut != null) {
            encryptingStreamOut.wrap(DisabledByteBuffer.INSTANCE); //prevent segmentation fault
            encryptingStreamOut = null;
        }
        if (encryptingStreamIn != null) {
            encryptingStreamIn.close();
            encryptingStreamIn = null;
        }
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        this.decryptedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.decryptedBuffer = null;
        }
    }

    @Override
    public int writeBuffer(final IByteBuffer dst) {
        encryptingStreamOut.wrap(dst.sliceFrom(PAYLOAD_INDEX));
        try {
            decryptedBuffer.getBytes(0, encryptingStreamIn);
            encryptingStreamIn.flush();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final int encryptedLength = encryptingStreamOut.position();
        dst.putInt(DECRYPTEDLENGTH_INDEX, decryptedBuffer.capacity());
        return PAYLOAD_INDEX + encryptedLength;
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
