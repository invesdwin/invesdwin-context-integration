package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.SslEngineStateMachine
 */
@NotThreadSafe
public class TlsSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final ISynchronousReader<IByteBuffer> underlyingReader;
    private final SSLEngine engine;

    private IByteBuffer unencryptedBuffer;
    private java.nio.ByteBuffer unencrypted;
    private final java.nio.ByteBuffer[] unencryptedArray = new java.nio.ByteBuffer[1];

    public TlsSynchronousReader(final SSLEngine engine, final ISynchronousReader<IByteBuffer> underlyingReader) {
        this.engine = engine;
        this.underlyingReader = underlyingReader;
    }

    @Override
    public void open() throws IOException {
        underlyingReader.open();

        unencryptedBuffer = ByteBuffers.allocateDirect(engine.getSession().getApplicationBufferSize());
        unencrypted = unencryptedBuffer.asNioByteBuffer();
        // eliminates array creation on each call to SSLEngine.wrap()
        unencryptedArray[0] = unencrypted;
    }

    @Override
    public void close() throws IOException {
        underlyingReader.close();

        unencryptedBuffer = null;
        unencrypted = null;
        unencryptedArray[0] = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return underlyingReader.hasNext();
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        try {
            final IByteBuffer encryptedBuffer = underlyingReader.readMessage();
            final java.nio.ByteBuffer encrypted = encryptedBuffer.asNioByteBuffer();
            final int encryptedPositionBefore = encrypted.position();
            OUTER: while (true) {
                final Status status = engine.unwrap(encrypted, unencryptedArray).getStatus();
                switch (status) {
                case OK:
                    break OUTER;
                case BUFFER_OVERFLOW:
                    final int unencryptedPositionBefore = unencrypted.position();
                    ByteBuffers.expand(unencryptedBuffer);
                    unencrypted = unencryptedBuffer.asNioByteBuffer();
                    unencryptedArray[0] = unencrypted;
                    ByteBuffers.position(unencrypted, unencryptedPositionBefore);
                    continue;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("Buffer underflow should not happen here");
                case CLOSED:
                    throw FastEOFException.getInstance("Socket closed");
                default:
                    throw UnknownArgumentException.newInstance(Status.class, status);
                }
            }
            ByteBuffers.position(encrypted, encryptedPositionBefore);
            return unencryptedBuffer.sliceTo(unencrypted.position());
        } finally {
            unencrypted.clear();
        }
    }

    @Override
    public void readFinished() {
        underlyingReader.readFinished();
    }

}
