package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;

@Disabled("not working")
@NotThreadSafe
public class TlsMinaSocketChannelTest extends MinaSocketChannelTest {

    @Override
    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsMinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize) {

            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                final InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
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
