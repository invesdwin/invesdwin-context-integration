package de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.IMac;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Encrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class AuthenticatedEncryptionSynchronousWriter
        implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IAuthenticationFactory authenticationFactory;
    private IByteBuffer buffer;
    private IByteBuffer decryptedBuffer;
    private IMac mac;

    public AuthenticatedEncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final IEncryptionFactory encryptionFactory, final IAuthenticationFactory authenticationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.authenticationFactory = authenticationFactory;
    }

    public ISynchronousWriter<IByteBufferWriter> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        mac = authenticationFactory.getAlgorithm().newMac();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
        if (mac != null) {
            mac.close();
            mac = null;
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
        final int signatureLength = authenticationFactory.putSignature(buffer, signatureIndex, mac);
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
