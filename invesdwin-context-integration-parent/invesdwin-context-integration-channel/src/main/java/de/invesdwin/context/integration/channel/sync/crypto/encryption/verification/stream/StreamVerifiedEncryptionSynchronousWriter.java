package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.stream.LayeredHashOutputStream;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.DisabledByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.stream.ExpandableByteBufferOutputStream;

/**
 * Encrypts multiple messages as if they are from a continuous stream. It is stateful to the connection and more
 * efficient due to object reuse.
 */
@NotThreadSafe
public class StreamVerifiedEncryptionSynchronousWriter
        implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    public static final int DECRYPTEDLENGTH_INDEX = 0;
    public static final int DECRYPTEDLENGTH_SIZE = Integer.BYTES;

    public static final int PAYLOAD_INDEX = DECRYPTEDLENGTH_INDEX + DECRYPTEDLENGTH_SIZE;

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer buffer;

    private IByteBuffer decryptedBuffer;

    private ExpandableByteBufferOutputStream encryptingStreamOut;
    private LayeredHashOutputStream signatureStreamIn;
    private OutputStream encryptingStreamIn;

    public StreamVerifiedEncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final IEncryptionFactory encryptionFactory, final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    public ISynchronousWriter<IByteBufferProvider> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        encryptingStreamOut = new ExpandableByteBufferOutputStream();
        signatureStreamIn = verificationFactory.newHashOutputStream(encryptingStreamOut);
        encryptingStreamIn = encryptionFactory.newStreamingEncryptor(signatureStreamIn);
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
            signatureStreamIn = null;
            encryptingStreamIn.close();
            encryptingStreamIn = null;
        }
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.decryptedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.decryptedBuffer = null;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        signatureStreamIn.init(); //in case of exceptions, it is lazy
        encryptingStreamOut.wrap(dst.sliceFrom(PAYLOAD_INDEX));
        try {
            decryptedBuffer.getBytes(0, encryptingStreamIn);
            encryptingStreamIn.flush();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final int encryptedLength = encryptingStreamOut.position();
        dst.putInt(DECRYPTEDLENGTH_INDEX, decryptedBuffer.capacity());
        final byte[] signature = signatureStreamIn.getHash().doFinal();
        final int signatureIndex = PAYLOAD_INDEX + encryptedLength;
        dst.putBytes(signatureIndex, signature);
        return signatureIndex + signature.length;
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
