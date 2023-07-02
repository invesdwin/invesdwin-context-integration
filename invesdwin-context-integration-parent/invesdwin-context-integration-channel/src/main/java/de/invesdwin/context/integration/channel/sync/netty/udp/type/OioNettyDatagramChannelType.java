package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.oio.OioDatagramChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@SuppressWarnings("deprecation")
@Immutable
public class OioNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final OioNettyDatagramChannelType INSTANCE = new OioNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new OioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new OioEventLoopGroup(threadCount);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return OioDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return OioDatagramChannel.class;
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
