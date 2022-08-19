package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.time.duration.Duration;

/**
 * Use this handshake if you only want to synchronize open of reader/writer across multiple threads. E.g. when sharing a
 * socket in bidirectional mode.
 */
@Immutable
public class DisabledHandshakeProvider implements IHandshakeProvider {

    private final Duration handshakeTimeout;

    public DisabledHandshakeProvider(final Duration handshakeTimeout) {
        this.handshakeTimeout = handshakeTimeout;
    }

    @Override
    public void handshake(final HandshakeChannel channel) throws IOException {
        final IgnoreOpenCloseSynchronousWriter<IByteBufferWriter> ignoreOpenCloseWriter = IgnoreOpenCloseSynchronousWriter
                .valueOf(channel.getWriter().getUnderlyingWriter());
        final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader = IgnoreOpenCloseSynchronousReader
                .valueOf(channel.getReader().getUnderlyingReader());
        channel.getWriter().setEncryptedWriter(ignoreOpenCloseWriter);
        channel.getReader().setEncryptedReader(ignoreOpenCloseReader);
    }

    @Override
    public Duration getHandshakeTimeout() {
        return handshakeTimeout;
    }

}
