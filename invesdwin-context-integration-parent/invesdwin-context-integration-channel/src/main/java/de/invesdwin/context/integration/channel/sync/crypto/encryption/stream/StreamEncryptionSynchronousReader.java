package de.invesdwin.context.integration.channel.sync.crypto.encryption.stream;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.security.crypto.encryption.IEncryptionFactory;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.stream.ByteBufferInputStream;

/**
 * Decrypt multiple messages as if they come from a continuous stream. It is stateful to the connection and more
 * efficient due to object reuse.
 */
@NotThreadSafe
public class StreamEncryptionSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> delegate;
    private final IEncryptionFactory encryptionFactory;
    private IByteBuffer decryptedBuffer;
    private ByteBufferInputStream decryptingStreamIn;
    private InputStream decryptingStreamOut;

    public StreamEncryptionSynchronousReader(final ISynchronousReader<IByteBuffer> delegate,
            final IEncryptionFactory encryptionFactory) {
        this.delegate = delegate;
        this.encryptionFactory = encryptionFactory;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
        decryptedBuffer = ByteBuffers.allocateExpandable();
        decryptingStreamIn = new ByteBufferInputStream();
        decryptingStreamOut = encryptionFactory.newStreamingDecryptor(decryptingStreamIn);
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
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer encryptedBuffer = delegate.readMessage();
        final int decryptedLength = encryptedBuffer.getInt(StreamEncryptionSynchronousWriter.DECRYPTEDLENGTH_INDEX);
        final IByteBuffer payloadBuffer = encryptedBuffer.sliceFrom(StreamEncryptionSynchronousWriter.PAYLOAD_INDEX);
        decryptingStreamIn.wrap(payloadBuffer);
        decryptedBuffer.putBytesTo(0, decryptingStreamOut, decryptedLength);
        return decryptedBuffer.sliceTo(decryptedLength);
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }
}
