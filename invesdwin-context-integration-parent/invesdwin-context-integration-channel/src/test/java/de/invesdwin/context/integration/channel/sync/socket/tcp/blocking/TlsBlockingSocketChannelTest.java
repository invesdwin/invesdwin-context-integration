package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.ITransportLayerSecurityProvider;

@NotThreadSafe
public class TlsBlockingSocketChannelTest extends BlockingSocketChannelTest {

    @Override
    protected BlockingSocketSynchronousChannel newBlockingSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new TlsBlockingSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize) {
            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider() {
                return new DerivedKeyTransportLayerSecurityProvider(getSocketAddress(), server) {
                    @Override
                    protected String getHostname() {
                        return getSocketAddress().getHostName();
                    }
                };
            }
        };
    }

}
