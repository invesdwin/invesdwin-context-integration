package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;

@NotThreadSafe
public class TlsBidiMinaSocketChannelTest extends BidiMinaSocketChannelTest {

    @Override
    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsMinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize) {

            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(socketAddress, server) {
                    @Override
                    protected String getHostname() {
                        return socketAddress.getHostName();
                    }
                };
            }

        };
    }

}
