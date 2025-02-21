package de.invesdwin.context.integration.channel.sync.hadronio.netty;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

@Immutable
public class HadroNioNettySocketChannelType implements INettySocketChannelType {

    public static final HadroNioNettySocketChannelType INSTANCE = new HadroNioNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup(final int threadCount) {
        return new HadroNioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final int threadCount, final SelectStrategyFactory selectStrategyFactory,
            final EventLoopGroup parentGroup) {
        return new HadroNioEventLoopGroup(threadCount);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup(final int threadCount,
            final SelectStrategyFactory selectStrategyFactory) {
        return new HadroNioEventLoopGroup(threadCount);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return HadroNioNettyServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return HadroNioNettySocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean lowLatency,
            final boolean server) {
        consumer.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) {
        //noop
    }
}
