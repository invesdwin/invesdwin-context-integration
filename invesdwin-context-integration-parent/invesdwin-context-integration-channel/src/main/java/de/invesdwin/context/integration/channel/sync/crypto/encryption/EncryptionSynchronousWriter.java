package de.invesdwin.context.integration.channel.sync.crypto.encryption;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Encrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class EncryptionSynchronousWriter implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decryptedBuffer;

    public EncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final IEncryptionFactory encryptionFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
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
    public void write(final IByteBufferProvider message) throws IOException {
        this.decryptedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.decryptedBuffer = null;
        }
    }

    @Override
    public boolean writeFinished() throws IOException {
        return delegate.writeFinished();
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        return encryptionFactory.encrypt(decryptedBuffer, dst);
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
