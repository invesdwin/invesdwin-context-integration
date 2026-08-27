package de.invesdwin.context.integration.channel.mina.sync;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.mina.sync.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;

@NotThreadSafe
public class TlsBidiMinaSocketChannelTest extends BidiMinaSocketChannelTest {

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
