package de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.pool.IMac;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Decrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class AuthenticatedEncryptionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IAuthenticationFactory authenticationFactory;
    private IByteBuffer decryptedBuffer;
    private IMac mac;

    public AuthenticatedEncryptionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IEncryptionFactory encryptionFactory, final IAuthenticationFactory authenticationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.authenticationFactory = authenticationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptedBuffer = ByteBuffers.allocateExpandable();
        mac = authenticationFactory.getAlgorithm().newMac();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decryptedBuffer = null;
        mac = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = delegate.readMessage();
        final IByteBuffer encryptedBuffer = authenticationFactory.verifyAndSlice(message, mac);
        encryptionFactory.decrypt(encryptedBuffer, decryptedBuffer);
        return decryptedBuffer;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}