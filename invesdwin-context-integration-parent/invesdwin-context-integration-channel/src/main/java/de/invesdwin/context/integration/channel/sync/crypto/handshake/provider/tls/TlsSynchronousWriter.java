package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
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
        final IByteBuffer messageBuffer = message.asBuffer();
        channel.setOutboundApplicationDataBuffer(messageBuffer);
    }

    @Override
    public boolean writeFinished() throws IOException {
        return !channel.action();
    }

}
