package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.TlsNettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;

/**
 * This test fails on netty-tcnative-boringssl-static. It says a message was unencrypted.
 */
@NotThreadSafe
public class TlsNettySocketChannelTest extends NettySocketChannelTest {

    @Override
    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsNettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize) {

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
