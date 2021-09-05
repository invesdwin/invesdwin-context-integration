package de.invesdwin.context.integration.channel.netty.udp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.socket.udp.blocking.ABlockingDatagramSocketSynchronousChannel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

@Immutable
public class NioNettyDatagramChannelType implements INettyDatagramChannelType {

    public static final NioNettyDatagramChannelType INSTANCE = new NioNettyDatagramChannelType();

    @Override
    public EventLoopGroup newServerWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public Class<? extends DatagramChannel> getServerChannelType() {
        return NioDatagramChannel.class;
    }

    @Override
    public Class<? extends DatagramChannel> getClientChannelType() {
        return NioDatagramChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        consumer.option(ChannelOption.IP_TOS, ABlockingDatagramSocketSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, socketSize);
    }

    @Override
    public void initChannel(final DatagramChannel channel, final boolean server) throws Exception {
        //noop
    }
}
