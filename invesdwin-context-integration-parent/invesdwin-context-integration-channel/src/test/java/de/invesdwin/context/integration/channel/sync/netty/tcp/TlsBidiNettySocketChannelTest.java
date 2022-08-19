package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.TlsNettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;

@NotThreadSafe
public class TlsBidiNettySocketChannelTest extends BidiNettySocketChannelTest {

    @Override
    protected NettySocketChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsNettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize) {

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

}
