package de.invesdwin.context.integration.channel.mina.async;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.mina.sync.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.mina.sync.TlsMinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.mina.sync.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;

@NotThreadSafe
public class TlsMinaSocketHandlerTest extends MinaSocketHandlerTest {

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
