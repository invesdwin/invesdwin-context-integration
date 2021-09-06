package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.ABlockingDatagramSocketSynchronousChannel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@Immutable
public class NioNettySocketChannelType implements INettySocketChannelType {

    public static final NioNettySocketChannelType INSTANCE = new NioNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup parentGroup) {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return NioServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return NioSocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        consumer.option(ChannelOption.SO_KEEPALIVE, true);
        consumer.option(ChannelOption.TCP_NODELAY, true);
        consumer.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
        //        consumer.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
        //                ContextProperties.DEFAULT_NETWORK_TIMEOUT.intValue(FTimeUnit.MILLISECONDS));
        //        consumer.option(ChannelOption.SO_TIMEOUT,
        //                ContextProperties.DEFAULT_NETWORK_TIMEOUT.intValue(FTimeUnit.MILLISECONDS));
        consumer.option(ChannelOption.IP_TOS, ABlockingDatagramSocketSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        //noop
    }
}
