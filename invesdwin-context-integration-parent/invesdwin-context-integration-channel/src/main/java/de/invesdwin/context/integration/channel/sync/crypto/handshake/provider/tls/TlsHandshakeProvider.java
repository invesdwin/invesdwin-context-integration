package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLEngine;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousReader;
import de.invesdwin.context.integration.channel.sync.IgnoreOpenCloseSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannel;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class TlsHandshakeProvider implements IHandshakeProvider {

    private final InetSocketAddress socketAddress;
    private final boolean server;
    private final Duration handshakeTimeout;

    public TlsHandshakeProvider(final Duration handshakeTimeout, final InetSocketAddress socketAddress,
            final boolean server) {
        this.handshakeTimeout = handshakeTimeout;
        this.socketAddress = socketAddress;
        this.server = server;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    @Override
    public void handshake(final HandshakeChannel channel) throws IOException {
        final ITransportLayerSecurityProvider tlsProvider = newTransportLayerSecurityProvider();
        final SSLEngine engine = tlsProvider.newEngine();
        final ISynchronousReader<IByteBuffer> underlyingReader = channel.getReader().getUnderlyingReader();
        final ISynchronousWriter<IByteBufferWriter> underlyingWriter = channel.getWriter().getUnderlyingWriter();
        final ASpinWait readerSpinWait = newSpinWait(underlyingReader);

        final TlsHandshaker handshaker = TlsHandshakerObjectPool.INSTANCE.borrowObject();
        try {
            handshaker.performHandshake(handshakeTimeout, socketAddress, tlsProvider.getProtocol(), engine,
                    readerSpinWait, underlyingReader, underlyingWriter);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            TlsHandshakerObjectPool.INSTANCE.returnObject(handshaker);
        }

        final IgnoreOpenCloseSynchronousWriter<IByteBufferWriter> ignoreOpenCloseWriter = IgnoreOpenCloseSynchronousWriter
                .valueOf(underlyingWriter);
        final IgnoreOpenCloseSynchronousReader<IByteBuffer> ignoreOpenCloseReader = IgnoreOpenCloseSynchronousReader
                .valueOf(underlyingReader);
        final TlsSynchronousChannel tlsChannel = new TlsSynchronousChannel(handshakeTimeout, engine,
                ignoreOpenCloseReader, ignoreOpenCloseWriter);
        final TlsSynchronousReader encryptedReader = new TlsSynchronousReader(tlsChannel);
        final TlsSynchronousWriter encryptedWriter = new TlsSynchronousWriter(tlsChannel);
        channel.getReader().setEncryptedReader(encryptedReader);
        channel.getWriter().setEncryptedWriter(encryptedWriter);
        encryptedReader.open();
        encryptedWriter.open();
    }

    protected ASpinWait newSpinWait(final ISynchronousReader<IByteBuffer> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.hasNext();
            }
        };
    }

    protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
        return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), isServer());
    }

    @Override
    public Duration getHandshakeTimeout() {
        return handshakeTimeout;
    }

}
