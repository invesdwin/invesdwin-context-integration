package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
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
    public EventLoopGroup newServerAcceptorGroup() {
        return new KQueueEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new KQueueEventLoopGroup(1, INettySocketChannelType.DEFAULT_SELECT_STRATEGY);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new KQueueEventLoopGroup(1, INettySocketChannelType.DEFAULT_SELECT_STRATEGY);
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
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        NioNettySocketChannelType.INSTANCE.channelOptions(consumer, socketSize, server);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        NioNettySocketChannelType.INSTANCE.initChannel(channel, server);
    }

}
