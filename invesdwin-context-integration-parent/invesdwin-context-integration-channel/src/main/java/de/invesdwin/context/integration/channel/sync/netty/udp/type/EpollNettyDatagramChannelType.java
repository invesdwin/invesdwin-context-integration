package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.SelectStrategyFactories;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@Immutable
public class EpollNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final EpollNettyDatagramChannelType INSTANCE = new EpollNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup() {
        return new EpollEventLoopGroup(1, SelectStrategyFactories.BUSY_WAIT);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new EpollEventLoopGroup(1, SelectStrategyFactories.BUSY_WAIT);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return EpollDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return EpollDatagramChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        NioNettyDatagramChannelType.INSTANCE.channelOptions(consumer, socketSize);
        //http://vger.kernel.org/netconf2019_files/udp_gro.pdf
        consumer.option(EpollChannelOption.UDP_GRO, true);
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) throws Exception {
        NioNettyDatagramChannelType.INSTANCE.initChannel(channel, server);
    }

}