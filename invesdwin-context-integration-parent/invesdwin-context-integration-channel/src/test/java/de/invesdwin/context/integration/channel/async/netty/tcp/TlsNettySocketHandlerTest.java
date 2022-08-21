package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.NettyDerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsNettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.buffer.ByteBufAllocator;

@NotThreadSafe
public class TlsNettySocketHandlerTest extends NettySocketHandlerTest {

    @Override
    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsNettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize) {
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
