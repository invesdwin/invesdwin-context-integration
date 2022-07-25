package de.invesdwin.context.integration.channel.sync.crypto.encryption.authentication.stream;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.authentication.IAuthenticationFactory;
import de.invesdwin.context.security.crypto.authentication.mac.IMac;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.stream.ByteBufferInputStream;

/**
 * Decrypt multiple messages as if they come from a continuous stream. It is stateful to the connection and more
 * efficient due to object reuse.
 */
@NotThreadSafe
public class StreamAuthenticatedEncryptionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IEncryptionFactory encryptionFactory;
    private final IAuthenticationFactory authenticationFactory;
    private IByteBuffer decryptedBuffer;
    private ByteBufferInputStream decryptingStreamIn;
    private InputStream decryptingStreamOut;
    private IMac mac;

    public StreamAuthenticatedEncryptionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IEncryptionFactory encryptionFactory, final IAuthenticationFactory authenticationFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
        this.authenticationFactory = authenticationFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptedBuffer = ByteBuffers.allocateExpandable();
        decryptingStreamIn = new ByteBufferInputStream();
        decryptingStreamOut = encryptionFactory.newDecryptor(decryptingStreamIn);
        mac = authenticationFactory.getAlgorithm().newMac();
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
        if (mac != null) {
            mac.close();
            mac = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer encryptedBuffer = delegate.readMessage();
        final int decryptedLength = encryptedBuffer
                .getInt(StreamAuthenticatedEncryptionSynchronousWriter.DECRYPTEDLENGTH_INDEX);
        final IByteBuffer payloadBuffer = mac.verifyAndSlice(encryptedBuffer);
        decryptingStreamIn.wrap(payloadBuffer);
        decryptedBuffer.putBytesTo(0, decryptingStreamOut, decryptedLength);

        return decryptedBuffer;
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }

}
