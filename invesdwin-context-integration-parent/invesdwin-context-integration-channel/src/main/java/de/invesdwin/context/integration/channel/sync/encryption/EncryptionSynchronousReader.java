package de.invesdwin.context.integration.channel.sync.encryption;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.streams.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Decrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class EncryptionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer decryptedBuffer;

    public EncryptionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IEncryptionFactory encryptionFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptedBuffer = ByteBuffers.allocateExpandable();
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
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer encryptedBuffer = delegate.readMessage();
        encryptionFactory.decrypt(encryptedBuffer, decryptedBuffer);
        return decryptedBuffer;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
