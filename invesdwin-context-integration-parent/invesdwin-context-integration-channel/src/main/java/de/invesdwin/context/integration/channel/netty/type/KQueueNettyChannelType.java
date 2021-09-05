package de.invesdwin.context.integration.channel.netty.type;

import javax.annotation.concurrent.Immutable;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-macosbsd-native-transport
 */
@Immutable
public class KQueueNettyChannelType implements INettyChannelType {

    public static final KQueueNettyChannelType INSTANCE = new KQueueNettyChannelType();

    @Override
    public EventLoopGroup newServerBossGroup() {
        return new KQueueEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new KQueueEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new KQueueEventLoopGroup(1);
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelType() {
        return KQueueServerSocketChannel.class;
    }

    @Override
    public Class<? extends Channel> getClientChannelType() {
        return KQueueSocketChannel.class;
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
