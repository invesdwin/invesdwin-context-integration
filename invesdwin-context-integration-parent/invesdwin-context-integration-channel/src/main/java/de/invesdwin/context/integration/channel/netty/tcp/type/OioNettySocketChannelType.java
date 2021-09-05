package de.invesdwin.context.integration.channel.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
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
    public EventLoopGroup newServerAcceptorGroup() {
        return new OioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new OioEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new OioEventLoopGroup(1);
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
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        NioNettySocketChannelType.INSTANCE.channelOptions(consumer, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        //noop
    }

}
