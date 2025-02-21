package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-macosbsd-native-transport
 */
@Immutable
public class KQueueNettySocketChannelType implements INettySocketChannelType {

    public static final KQueueNettySocketChannelType INSTANCE = new KQueueNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup(final int threadCount) {
        return new KQueueEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount, final SelectStrategyFactory selectStrategyFactory,
            final EventLoopGroup bossGroup) {
        return new KQueueEventLoopGroup(threadCount, selectStrategyFactory);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new KQueueEventLoopGroup(threadCount, selectStrategyFactory);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return KQueueServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return KQueueSocketChannel.class;
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
