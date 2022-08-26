package de.invesdwin.context.integration.channel.sync.crypto.encryption;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Decrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class EncryptionSynchronousReader implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer decryptedBuffer;

    public EncryptionSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
            final IEncryptionFactory encryptionFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decryptedBuffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        if (decryptedBuffer == null) {
            decryptedBuffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(decryptedBuffer);
        return decryptedBuffer.sliceTo(length);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final IByteBuffer encryptedBuffer = delegate.readMessage().asBuffer();
        final int decryptedSize = encryptionFactory.decrypt(encryptedBuffer, dst);
        return decryptedSize;
    }
}
