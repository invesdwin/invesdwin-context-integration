package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;

@NotThreadSafe
public class TlsBidiMinaSocketChannelTest extends BidiMinaSocketChannelTest {

    @Disabled
    @Override
    public void testBidiMinaVmPipeChannelPerformance() throws InterruptedException {
        super.testBidiMinaVmPipeChannelPerformance();
    }

    @Override
    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        return new TlsMinaSocketSynchronousChannel(type, inetSocketAddress, server, estimatedMaxMessageSize) {
            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(inetSocketAddress, server) {
                    @Override
                    protected String getHostname() {
                        return inetSocketAddress.getHostName();
                    }
                };
            }

        };
    }

}
