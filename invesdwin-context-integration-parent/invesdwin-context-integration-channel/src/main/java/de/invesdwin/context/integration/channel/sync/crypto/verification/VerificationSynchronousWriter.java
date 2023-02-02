package de.invesdwin.context.integration.channel.sync.crypto.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Signs each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerificationSynchronousWriter implements ISynchronousWriter<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousWriter<IByteBufferProvider> delegate;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer buffer;
    private IByteBuffer unsignedBuffer;
    private IHash hash;

    public VerificationSynchronousWriter(final ISynchronousWriter<IByteBufferProvider> delegate,
            final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.verificationFactory = verificationFactory;
    }

    public ISynchronousWriter<IByteBufferProvider> getDelegate() {
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
    public boolean writeReady() throws IOException {
        return delegate.writeReady();
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        this.unsignedBuffer = message.asBuffer();
        delegate.write(this);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (delegate.writeFlushed()) {
            this.unsignedBuffer = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        //Sadly we need to copy here. E.g. StreamVerificationEncryptionSynchronousWriter spares a copy by doing this together
        return verificationFactory.copyAndHash(unsignedBuffer, dst, hash);
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
