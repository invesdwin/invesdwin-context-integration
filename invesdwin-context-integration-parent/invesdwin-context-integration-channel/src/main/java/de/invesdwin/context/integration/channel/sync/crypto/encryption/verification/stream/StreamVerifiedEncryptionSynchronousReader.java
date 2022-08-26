package de.invesdwin.context.integration.channel.sync.crypto.encryption.verification.stream;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.context.security.crypto.verification.IVerificationFactory;
import de.invesdwin.context.security.crypto.verification.hash.IHash;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.stream.ByteBufferInputStream;

/**
 * Decrypt multiple messages as if they come from a continuous stream. It is stateful to the connection and more
 * efficient due to object reuse.
 */
@NotThreadSafe
public class StreamVerifiedEncryptionSynchronousReader
        implements ISynchronousReader<IByteBufferProvider>, IByteBufferProvider {

    private final ISynchronousReader<IByteBufferProvider> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IVerificationFactory verificationFactory;
    private IByteBuffer decryptedBuffer;
    private ByteBufferInputStream decryptingStreamIn;
    private InputStream decryptingStreamOut;
    private IHash hash;

    public StreamVerifiedEncryptionSynchronousReader(final ISynchronousReader<IByteBufferProvider> delegate,
            final IEncryptionFactory encryptionFactory, final IVerificationFactory verificationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.verificationFactory = verificationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptingStreamIn = new ByteBufferInputStream();
        decryptingStreamOut = encryptionFactory.newStreamingDecryptor(decryptingStreamIn);
        hash = verificationFactory.getAlgorithm().newHash();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        decryptedBuffer = null;
        decryptingStreamIn = null;
        if (decryptingStreamOut != null) {
            decryptingStreamOut.close();
            decryptingStreamOut = null;
        }
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
        if (decryptedBuffer == null) {
            decryptedBuffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(decryptedBuffer);
        return decryptedBuffer.sliceTo(length);
    }

    @Override
    public int getBuffer(final IByteBuffer dst) throws IOException {
        final IByteBuffer encryptedBuffer = delegate.readMessage().asBuffer();
        final int decryptedLength = encryptedBuffer
                .getInt(StreamVerifiedEncryptionSynchronousWriter.DECRYPTEDLENGTH_INDEX);
        final IByteBuffer payloadBuffer = verificationFactory.verifyAndSlice(
                encryptedBuffer.sliceFrom(StreamVerifiedEncryptionSynchronousWriter.PAYLOAD_INDEX), hash);
        decryptingStreamIn.wrap(payloadBuffer);
        dst.putBytesTo(0, decryptingStreamOut, decryptedLength);
        return decryptedLength;
    }

}
