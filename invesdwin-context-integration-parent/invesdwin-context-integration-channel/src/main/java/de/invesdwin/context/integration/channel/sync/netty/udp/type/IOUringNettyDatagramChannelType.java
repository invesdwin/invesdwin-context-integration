package de.invesdwin.context.integration.channel.sync.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.incubator.channel.uring.IOUringDatagramChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@Immutable
public class IOUringNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final IOUringNettyDatagramChannelType INSTANCE = new IOUringNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup() {
        return new IOUringEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new IOUringEventLoopGroup(1);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return IOUringDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return IOUringDatagramChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        NioNettyDatagramChannelType.INSTANCE.channelOptions(consumer, socketSize, server);
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) throws Exception {
        NioNettyDatagramChannelType.INSTANCE.initChannel(channel, server);
    }

}
