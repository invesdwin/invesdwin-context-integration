package de.invesdwin.context.integration.channel.netty.sync.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.netty.sync.crypto.NettyDerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.netty.sync.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import io.netty.buffer.ByteBufAllocator;

@NotThreadSafe
public class TlsBidiNettySocketChannelTest extends BidiNettySocketChannelTest {

    @Override
    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new TlsNettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency) {

            @Override
            protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider(final ByteBufAllocator alloc) {
                return new NettyDerivedKeyTransportLayerSecurityProvider(socketAddress, server) {
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
