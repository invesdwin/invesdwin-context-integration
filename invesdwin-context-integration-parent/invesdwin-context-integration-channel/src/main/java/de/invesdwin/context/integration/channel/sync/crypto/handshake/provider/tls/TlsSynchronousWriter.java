package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.Status;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.SslEngineStateMachine
 */
@NotThreadSafe
public class TlsSynchronousWriter implements ISynchronousWriter<IByteBufferWriter> {

    private final ISynchronousWriter<IByteBufferWriter> underlyingWriter;
    private final SSLEngine engine;

    private java.nio.ByteBuffer encrypted;
    private IByteBuffer encryptedBuffer;
    // eliminates array creation on each call to SSLEngine.wrap()
    private final java.nio.ByteBuffer[] unencryptedArray = new java.nio.ByteBuffer[0];

    public TlsSynchronousWriter(final SSLEngine engine, final ISynchronousWriter<IByteBufferWriter> underlyingWriter) {
        this.engine = engine;
        this.underlyingWriter = underlyingWriter;
    }

    @Override
    public void open() throws IOException {
        underlyingWriter.open();

        encryptedBuffer = ByteBuffers.allocateDirectExpandable(engine.getSession().getPacketBufferSize());
        encrypted = encryptedBuffer.asNioByteBuffer();
    }

    @Override
    public void close() throws IOException {
        underlyingWriter.close();

        encryptedBuffer = null;
        encrypted = null;
        unencryptedArray[0] = null;
    }

    @Override
    public void write(final IByteBufferWriter message) throws IOException {
        try {
            final IByteBuffer unencryptedBuffer = message.asBuffer();
            final java.nio.ByteBuffer unencrypted = unencryptedBuffer.asNioByteBuffer();
            final int unencryptedPositionBefore = unencrypted.position();
            OUTER: while (true) {
                unencryptedArray[0] = unencrypted;
                final Status status = engine.wrap(unencryptedArray, encrypted).getStatus();
                //                unencryptedArray[0] = null;
                switch (status) {
                case OK:
                    break OUTER;
                case BUFFER_OVERFLOW:
                    final int encryptedPositionBefore = encrypted.position();
                    ByteBuffers.expand(encryptedBuffer);
                    encrypted = encryptedBuffer.asNioByteBuffer();
                    ByteBuffers.position(encrypted, encryptedPositionBefore);
                    continue;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("Buffer underflow should not happen here");
                case CLOSED:
                    throw FastEOFException.getInstance("Socket closed");
                default:
                    throw UnknownArgumentException.newInstance(Status.class, status);
                }
            }
            ByteBuffers.position(unencrypted, unencryptedPositionBefore);
            underlyingWriter.write(encryptedBuffer.sliceTo(encrypted.position()));
        } finally {
            encrypted.clear();
        }
    }

    public void writeFully(final ChronicleSocketChannel dst, final java.nio.ByteBuffer byteBuffer) throws IOException {
        int remaining = byteBuffer.remaining();
        final int positionBefore = byteBuffer.position();
        while (remaining > 0) {
            final int count = dst.write(byteBuffer);
            if (count == -1) { // EOF
                break;
            }
            remaining -= count;
        }
        ByteBuffers.position(byteBuffer, positionBefore);
        if (remaining > 0) {
            throw ByteBuffers.newPutBytesToEOF();
        }
    }

}
