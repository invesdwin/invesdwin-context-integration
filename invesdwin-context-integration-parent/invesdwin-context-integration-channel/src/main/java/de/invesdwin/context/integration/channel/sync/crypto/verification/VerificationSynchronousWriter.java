package de.invesdwin.context.integration.channel.sync.crypto.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

/**
 * Signs each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerificationSynchronousWriter implements ISynchronousWriter<IByteBufferWriter>, IByteBufferWriter {

    private final ISynchronousWriter<IByteBufferWriter> delegate;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer buffer;
    private IByteBuffer unsignedBuffer;
    private IHash hash;

    public VerificationSynchronousWriter(final ISynchronousWriter<IByteBufferWriter> delegate,
            final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
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
        this.unsignedBuffer = message.asBuffer();
        try {
            delegate.write(this);
        } finally {
            this.unsignedBuffer = null;
        }
    }

    @Override
    public int writeBuffer(final IByteBuffer buffer) {
        //Sadly we need to copy here. E.g. StreamVerificationEncryptionSynchronousWriter spares a copy by doing this together
        return verificationFactory.copyAndHash(unsignedBuffer, buffer, hash);
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
