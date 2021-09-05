package de.invesdwin.context.integration.channel.netty.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.socket.udp.blocking.ABlockingDatagramSocketSynchronousChannel;
import de.invesdwin.util.time.date.FTimeUnit;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@Immutable
public class NioNettyChannelType implements INettyChannelType {

    public static final NioNettyChannelType INSTANCE = new NioNettyChannelType();

    @Override
    public EventLoopGroup newServerBossGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup parentGroup) {
        return new NioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new NioEventLoopGroup(1);
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelType() {
        return NioServerSocketChannel.class;
    }

    @Override
    public Class<? extends Channel> getClientChannelType() {
        return NioSocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        consumer.option(ChannelOption.SO_KEEPALIVE, true);
        consumer.option(ChannelOption.TCP_NODELAY, true);
        consumer.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
        consumer.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                ContextProperties.DEFAULT_NETWORK_TIMEOUT.intValue(FTimeUnit.MILLISECONDS));
        consumer.option(ChannelOption.IP_TOS, ABlockingDatagramSocketSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        consumer.option(ChannelOption.SO_SNDBUF, socketSize);
        consumer.option(ChannelOption.SO_RCVBUF, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        //noop
    }
}
