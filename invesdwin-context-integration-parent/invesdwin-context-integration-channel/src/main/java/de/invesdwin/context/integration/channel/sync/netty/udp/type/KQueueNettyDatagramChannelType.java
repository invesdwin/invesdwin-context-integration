package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-macosbsd-native-transport
 */
@Immutable
public class KQueueNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final KQueueNettyDatagramChannelType INSTANCE = new KQueueNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new KQueueEventLoopGroup(threadCount, selectStrategyFactory);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new KQueueEventLoopGroup(threadCount, selectStrategyFactory);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return KQueueDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return KQueueDatagramChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        NioNettyDatagramChannelType.INSTANCE.channelOptions(consumer, socketSize, server);
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) {
        NioNettyDatagramChannelType.INSTANCE.initChannel(channel, server);
    }

}
