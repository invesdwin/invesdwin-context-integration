package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

@Immutable
public class HadroNioNettySocketChannelType implements INettySocketChannelType {

    public static final HadroNioNettySocketChannelType INSTANCE = new HadroNioNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup() {
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
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return HadroNioNettyServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return HadroNioNettySocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize, final boolean server) {
        consumer.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        //noop
    }
}
