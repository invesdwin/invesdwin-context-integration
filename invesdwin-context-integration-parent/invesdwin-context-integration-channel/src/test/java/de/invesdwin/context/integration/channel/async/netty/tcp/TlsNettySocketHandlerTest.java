package de.invesdwin.context.integration.channel.async.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.TlsNettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;

@NotThreadSafe
public class TlsNettySocketHandlerTest extends NettySocketHandlerTest {

    @Override
    protected NettySocketChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new TlsNettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize) {
            @Override
            protected String getHostname() {
                return socketAddress.getHostName();
            }
        };
    }

}
