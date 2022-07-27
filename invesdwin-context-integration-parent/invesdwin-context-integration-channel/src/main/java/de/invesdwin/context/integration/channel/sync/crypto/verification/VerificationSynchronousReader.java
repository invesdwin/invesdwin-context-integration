package de.invesdwin.context.integration.channel.sync.crypto.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Verifies each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerificationSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IVerificationFactory verificationFactory;
    private IHash hash;

    public VerificationSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        this.hash = verificationFactory.getAlgorithm().newHash();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
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
        final IByteBuffer signedBuffer = delegate.readMessage();
        //no copy needed here
        return verificationFactory.verifyAndSlice(signedBuffer, hash);
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
