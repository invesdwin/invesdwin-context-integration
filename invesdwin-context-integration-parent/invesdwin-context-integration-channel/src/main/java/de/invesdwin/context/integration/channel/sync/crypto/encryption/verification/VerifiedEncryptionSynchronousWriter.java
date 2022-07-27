package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Encrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerifiedEncryptionSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer buffer;
    private IByteBuffer decryptedBuffer;
    private IHash hash;

    public VerifiedEncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final IEncryptionFactory encryptionFactory, final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    public ISynchronousWriter<IByteBufferWriter> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        hash = verificationFactory.getAlgorithm().newHash();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
        if (hash != null) {
            hash.close();
            hash = null;
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
    public int writeBuffer(final IByteBuffer buffer) {
        final int signatureIndex = encryptionFactory.encrypt(decryptedBuffer, buffer);
        final int signatureLength = verificationFactory.putHash(buffer, signatureIndex, hash);
        return signatureIndex + signatureLength;
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
