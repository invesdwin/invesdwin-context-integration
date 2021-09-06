package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@Immutable
public class EpollNettySocketChannelType implements INettySocketChannelType {

    public static final EpollNettySocketChannelType INSTANCE = new EpollNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup() {
        return new EpollEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new EpollEventLoopGroup(1, SelectStrategyFactories.SPIN_WAIT);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new EpollEventLoopGroup(1, SelectStrategyFactories.SPIN_WAIT);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return EpollServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return EpollSocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        NioNettySocketChannelType.INSTANCE.channelOptions(consumer, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        NioNettySocketChannelType.INSTANCE.initChannel(channel, server);
    }

}
