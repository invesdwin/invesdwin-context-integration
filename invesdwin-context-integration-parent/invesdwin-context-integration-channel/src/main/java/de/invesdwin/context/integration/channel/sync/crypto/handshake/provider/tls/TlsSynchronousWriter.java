package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Adapted from: net.openhft.chronicle.network.ssl.SslEngineStateMachine
 */
@NotThreadSafe
public class TlsSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final TlsSynchronousChannel channel;

    public TlsSynchronousWriter(final TlsSynchronousChannel channel) {
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
    public void write(final IByteBufferProvider message) throws IOException {
        final java.nio.ByteBuffer dst = channel.getOutboundApplicationData();
        if (dst.position() > 0) {
            while (channel.action()) {
                continue;
            }
        }
        final IByteBuffer messageBuffer = message.asBuffer();
        int srcRemaining = messageBuffer.capacity();
        int srcPosition = 0;
        int dstPosition = dst.position();
        dst.putInt(dstPosition + TlsSynchronousChannel.SIZE_INDEX, srcRemaining);
        dstPosition += TlsSynchronousChannel.SIZE_SIZE;
        while (srcRemaining > 0) {
            final int length = Integers.min(dst.remaining(), srcRemaining);
            messageBuffer.getBytes(srcPosition, dst, dstPosition, length);
            srcRemaining -= length;
            srcPosition += length;
            dstPosition += length;
            ByteBuffers.position(dst, dstPosition);
            if (srcRemaining > 0) {
                while (channel.action()) {
                    continue;
                }
                dstPosition = dst.position();
            }
        }
        while (channel.action()) {
            continue;
        }
    }

}
