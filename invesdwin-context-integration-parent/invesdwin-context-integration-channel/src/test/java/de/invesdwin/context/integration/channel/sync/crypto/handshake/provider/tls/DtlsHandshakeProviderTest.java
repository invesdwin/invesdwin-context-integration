package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.File;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.ITlsProtocol;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.protocol.TlsProtocol;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DtlsHandshakeProviderTest extends ALatencyChannelTest {

    @Test
    public void testDtlsHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        //we need to block here because multiple messages are written in succession
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testDtlsHandshakePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testDtlsHandshakePerformance_response.pipe", tmpfs, pipes);
        final InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        final TlsProtocol protocol = TlsProtocol.DTLS;
        final HandshakeChannelFactory serverHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, true, protocol));
        final HandshakeChannelFactory clientHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, false, protocol));
        runLatencyTest(pipes, requestFile, responseFile, null, null, serverHandshake, clientHandshake);
    }

    @Test
    public void testSizedTlsHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        //we need to block here because multiple messages are written in succession
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testSizedTlsHandshakePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testSizedTlsHandshakePerformance_response.pipe", tmpfs, pipes);
        final InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        final TlsProtocol protocol = TlsProtocol.DTLS;
        final HandshakeChannelFactory serverHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, true, protocol));
        final HandshakeChannelFactory clientHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, false, protocol));
        runLatencyTest(pipes, requestFile, responseFile, null, null, serverHandshake, clientHandshake);
    }

    private IHandshakeProvider newTlsHandshakeProvider(final Duration handshakeTimeout,
            final InetSocketAddress socketAddress, final boolean server, final ITlsProtocol protocol) {
        return new TlsHandshakeProvider(handshakeTimeout, socketAddress, server) {
            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), isServer()) {
                    @Override
                    protected String getHostname() {
                        return getSocketAddress().getHostName();
                    }

                    @Override
                    protected Integer getMaximumPacketSize() {
                        return DtlsHandshakeProviderTest.this.getMaxMessageSize();
                    }

                    @Override
                    public ITlsProtocol getProtocol() {
                        return protocol;
                    }
                };
            }
        };
    }

    @Override
    protected int getMaxMessageSize() {
        //TlsSynchronousChannel should manage this
        return SynchronousChannels.MAX_UNFRAGMENTED_DATAGRAM_PACKET_SIZE;
    }

}
