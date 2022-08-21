package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.SslEngineStateMachine
 */
@NotThreadSafe
public class TlsSynchronousReader implements ISynchronousReader<IByteBuffer> {

    private final TlsSynchronousChannel channel;

    public TlsSynchronousReader(final TlsSynchronousChannel channel) {
        this.channel = channel;
    }

    @Override
    public void open() throws IOException {
        channel.open();
        while (channel.action()) {
            continue;
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        while (channel.action()) {
            continue;
        }
        final java.nio.ByteBuffer src = channel.getInboundApplicationData();
        final int srcPosition = src.position();
        if (srcPosition < TlsSynchronousChannel.MESSAGE_INDEX) {
            return false;
        }
        /*
         * channel uses expandable buffer, so just wait until message is complete
         * 
         * (an outer fragment handler should make sure we don't exceed memory limits)
         */
        final int targetPosition = TlsSynchronousChannel.MESSAGE_INDEX + src.getInt(TlsSynchronousChannel.SIZE_INDEX);
        return srcPosition >= targetPosition;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final java.nio.ByteBuffer src = channel.getInboundApplicationData();
        src.flip();
        final IByteBuffer srcBuffer = channel.getInboundApplicationDataBuffer();
        final int messageLength = src.getInt(TlsSynchronousChannel.SIZE_INDEX);
        final IByteBuffer messageBuffer = srcBuffer.slice(TlsSynchronousChannel.MESSAGE_INDEX, messageLength);
        ByteBuffers.position(src, TlsSynchronousChannel.MESSAGE_INDEX + messageLength);
        return messageBuffer;
    }

    @Override
    public void readFinished() {
        final java.nio.ByteBuffer src = channel.getInboundApplicationData();
        if (src.hasRemaining()) {
            src.compact();
        } else {
            src.clear();
        }
    }

}
