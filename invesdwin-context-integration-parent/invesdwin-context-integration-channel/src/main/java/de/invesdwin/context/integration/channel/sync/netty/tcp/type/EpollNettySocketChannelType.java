package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
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
    public EventLoopGroup newServerAcceptorGroup(final int threadCount) {
        return new EpollEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount, final SelectStrategyFactory selectStrategyFactory,
            final EventLoopGroup parentGroup) {
        return new EpollEventLoopGroup(threadCount, selectStrategyFactory);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new EpollEventLoopGroup(threadCount, selectStrategyFactory);
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
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean lowLatency,
            final boolean server) {
        NioNettySocketChannelType.INSTANCE.channelOptions(consumer, socketSize, lowLatency, server);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) {
        NioNettySocketChannelType.INSTANCE.initChannel(channel, server);
    }

}
