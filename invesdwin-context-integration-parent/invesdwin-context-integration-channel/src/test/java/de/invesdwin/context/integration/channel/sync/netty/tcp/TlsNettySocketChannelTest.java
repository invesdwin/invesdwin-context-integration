package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.NettyDerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.buffer.ByteBufAllocator;

/**
 * This test fails on netty-tcnative-boringssl-static. It says a message was unencrypted.
 */
@NotThreadSafe
public class TlsNettySocketChannelTest extends NettySocketChannelTest {

    @Override
    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new TlsNettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency) {

            @Override
            public boolean isStreaming() {
                //dunno why this does not work in this test
                return false;
            }

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

                    //                    @Override
                    //                    protected SslProvider getEngineSslProvider() {
                    //                        return SslProvider.OPENSSL;
                    //                    }
                };
            }
        };
    }

}
