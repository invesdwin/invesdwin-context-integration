package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Decrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerifiedEncryptionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer decryptedBuffer;
    private IHash hash;

    public VerifiedEncryptionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IEncryptionFactory encryptionFactory, final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptedBuffer = ByteBuffers.allocateExpandable();
        hash = verificationFactory.getAlgorithm().newHash();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decryptedBuffer = null;
        if (hash != null) {
            hash.close();
            hash = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = delegate.readMessage();
        final IByteBuffer encryptedBuffer = verificationFactory.verifyAndSlice(message, hash);
        final int decryptedSize = encryptionFactory.decrypt(encryptedBuffer, decryptedBuffer);
        return decryptedBuffer.sliceTo(decryptedSize);
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
