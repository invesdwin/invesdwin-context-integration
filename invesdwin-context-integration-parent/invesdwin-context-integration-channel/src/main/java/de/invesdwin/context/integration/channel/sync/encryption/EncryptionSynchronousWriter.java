package de.invesdwin.context.integration.channel.sync.encryption;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.streams.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Encrypts each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class EncryptionSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer buffer;

    private IByteBuffer decryptedBuffer;

    public EncryptionSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
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
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        buffer = null;
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
        return encryptionFactory.encrypt(decryptedBuffer, buffer);
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
