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
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.HandshakeValidation;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
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
        final HandshakeValidation handshakeValidation = tlsProvider.getHandshakeValidation();

        final IgnoreOpenCloseSynchronousWriter<IByteBufferProvider> underlyingWriter = IgnoreOpenCloseSynchronousWriter
                .valueOf(channel.getWriter().getUnderlyingWriter());
        final IgnoreOpenCloseSynchronousReader<IByteBufferProvider> underlyingReader = IgnoreOpenCloseSynchronousReader
                .valueOf(channel.getReader().getUnderlyingReader());
        final ASpinWait readerSpinWait = newSpinWait(underlyingReader);
        final ASpinWait writerSpinWait = newSpinWait(underlyingWriter);
        final TlsSynchronousChannel tlsChannel = new TlsSynchronousChannel(handshakeTimeout,
                newHandshakeTimeoutRecoveryTries(), socketAddress, tlsProvider.getProtocol(), engine, underlyingReader,
                readerSpinWait, underlyingWriter, writerSpinWait, handshakeValidation);
        final TlsSynchronousReader encryptedReader = new TlsSynchronousReader(tlsChannel);
        final TlsSynchronousWriter encryptedWriter = new TlsSynchronousWriter(tlsChannel);
        channel.getReader().setEncryptedReader(encryptedReader);
        channel.getWriter().setEncryptedWriter(encryptedWriter);
        encryptedReader.open();
        encryptedWriter.open();
    }

    /**
     * Return null to disable. This will only be used for DTLS.
     */
    protected Integer newHandshakeTimeoutRecoveryTries() {
        return 3;
    }

    protected ASpinWait newSpinWait(final ISynchronousReader<IByteBufferProvider> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.hasNext();
            }
        };
    }

    protected ASpinWait newSpinWait(final ISynchronousWriter<IByteBufferProvider> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.writeFinished();
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
