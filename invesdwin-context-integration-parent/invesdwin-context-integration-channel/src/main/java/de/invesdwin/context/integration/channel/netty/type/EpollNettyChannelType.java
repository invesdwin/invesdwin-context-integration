package de.invesdwin.context.integration.channel.netty.type;

import javax.annotation.concurrent.Immutable;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@Immutable
public class EpollNettyChannelType implements INettyChannelType {

    public static final EpollNettyChannelType INSTANCE = new EpollNettyChannelType();

    @Override
    public EventLoopGroup newServerBossGroup() {
        return new EpollEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new EpollEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new EpollEventLoopGroup(1);
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelType() {
        return EpollServerSocketChannel.class;
    }

    @Override
    public Class<? extends Channel> getClientChannelType() {
        return EpollSocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        NioNettyChannelType.INSTANCE.channelOptions(consumer, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        //noop
    }

}
