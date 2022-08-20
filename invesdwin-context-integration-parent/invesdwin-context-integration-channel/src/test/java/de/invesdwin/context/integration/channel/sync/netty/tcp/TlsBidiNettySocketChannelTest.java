package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.buffer.ByteBufAllocator;

@NotThreadSafe
public class TlsBidiNettySocketChannelTest extends BidiNettySocketChannelTest {

    @Override
    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsNettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize) {

            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider(final ByteBufAllocator alloc) {
                return new DerivedKeyTransportLayerSecurityProvider(socketAddress, server) {
                    @Override
                    protected ByteBufAllocator getByteBufAllocator() {
                        return alloc;
                    }

                    @Override
                    protected String getHostname() {
                        return socketAddress.getHostName();
                    }

                    //            @Override
                    //            protected SslProvider getSslProvider() {
                    //                return SslProvider.OPENSSL;
                    //            }
                };
            }

        };
    }

}
