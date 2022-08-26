package de.invesdwin.context.integration.channel.sync.crypto.verification;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Verifies each message separately. Stateless regarding the connection.
 */
@NotThreadSafe
public class VerificationSynchronousReader implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final IVerificationFactory verificationFactory;
    private IHash hash;

    public VerificationSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
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
    public IByteBufferProvider readMessage() throws IOException {
        return this;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }

    @Override
    public IByteBuffer asBuffer() throws IOException {
        final IByteBuffer signedBuffer = delegate.readMessage().asBuffer();
        //no copy needed here
        return verificationFactory.verifyAndSlice(signedBuffer, hash);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final int signedBufferLength = delegate.readMessage().getBuffer(dst);
        //no copy needed here
        final IByteBuffer signedBuffer = dst.sliceTo(signedBufferLength);
        final IByteBuffer slice = verificationFactory.verifyAndSlice(signedBuffer, hash);
        assert slice.wrapAdjustment() == signedBuffer
                .wrapAdjustment() : "WrapAdjustment should not be different because hash should normally always be at the end of the buffer: slice.wrapAdjustment["
                        + slice.wrapAdjustment() + "] != signedBuffer.wrapAdjustment[" + signedBuffer.wrapAdjustment()
                        + "]";
        return slice.capacity();
    }
}
