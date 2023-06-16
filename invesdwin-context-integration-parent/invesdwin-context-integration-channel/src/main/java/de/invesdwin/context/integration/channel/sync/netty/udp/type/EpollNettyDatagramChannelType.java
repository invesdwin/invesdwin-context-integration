package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.channel.EventLoopGroup;
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
    public EventLoopGroup newServerWorkerGroup(final int threadCount) {
        return new EpollEventLoopGroup(threadCount, INettySocketChannelType.DEFAULT_SELECT_STRATEGY);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount) {
        return new EpollEventLoopGroup(threadCount, INettySocketChannelType.DEFAULT_SELECT_STRATEGY);
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
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        NioNettyDatagramChannelType.INSTANCE.channelOptions(consumer, socketSize, server);
        //http://vger.kernel.org/netconf2019_files/udp_gro.pdf
        //        consumer.option(EpollChannelOption.UDP_GRO, true);
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) throws Exception {
        NioNettyDatagramChannelType.INSTANCE.initChannel(channel, server);
    }

}
