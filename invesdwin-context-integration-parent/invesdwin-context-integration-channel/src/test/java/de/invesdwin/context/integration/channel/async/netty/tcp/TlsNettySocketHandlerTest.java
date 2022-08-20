package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.TlsNettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;

@NotThreadSafe
public class TlsNettySocketHandlerTest extends NettySocketHandlerTest {

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
            //                return SslProvider.OPENSSL_REFCNT;
            //            }
        };
    }

}
