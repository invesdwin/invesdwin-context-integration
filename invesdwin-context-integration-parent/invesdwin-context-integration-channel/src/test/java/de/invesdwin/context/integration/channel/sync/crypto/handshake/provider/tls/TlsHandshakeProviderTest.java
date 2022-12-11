package de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls;

import java.io.File;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.HandshakeChannelFactory;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.IHandshakeProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class TlsHandshakeProviderTest extends AChannelTest {

    @Test
    public void testTlsHandshakePerformance() throws InterruptedException {
        final boolean tmpfs = true;
        //handshake will get confused if blocking is not used
        final FileChannelType pipes = FileChannelType.BLOCKING_MAPPED;
        final File requestFile = newFile("testTlsHandshakePerformance_request.pipe", tmpfs, pipes);
        final File responseFile = newFile("testTlsHandshakePerformance_response.pipe", tmpfs, pipes);
        final InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        final HandshakeChannelFactory serverHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, true));
        final HandshakeChannelFactory clientHandshake = new HandshakeChannelFactory(
                newTlsHandshakeProvider(MAX_WAIT_DURATION, address, false));
        runPerformanceTest(pipes, requestFile, responseFile, null, null, serverHandshake, clientHandshake);
    }

    private IHandshakeProvider newTlsHandshakeProvider(final Duration handshakeTimeout,
            final InetSocketAddress socketAddress, final boolean server) {
        return new TlsHandshakeProvider(handshakeTimeout, socketAddress, server) {
            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), isServer()) {
                    @Override
                    protected String getHostname() {
                        return getSocketAddress().getHostName();
                    }
                };
            }
        };
    }

    @Override
    protected int getMaxMessageSize() {
        return 1324;
    }

}
