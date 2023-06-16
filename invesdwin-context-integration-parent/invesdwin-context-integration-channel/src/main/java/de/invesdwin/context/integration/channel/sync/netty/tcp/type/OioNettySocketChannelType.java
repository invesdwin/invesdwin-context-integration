package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;

@SuppressWarnings("deprecation")
@Immutable
public class OioNettySocketChannelType implements INettySocketChannelType {

    public static final OioNettySocketChannelType INSTANCE = new OioNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup(final int threadCount) {
        return new OioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount, final SelectStrategyFactory selectStrategyFactory,
            final EventLoopGroup bossGroup) {
        return new OioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new OioEventLoopGroup(threadCount);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return OioServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return OioSocketChannel.class;
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
