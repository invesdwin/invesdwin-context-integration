package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;

/**
 * https://netty.io/wiki/native-transports.html#using-the-linux-native-transport
 */
@Immutable
public class IOUringNettySocketChannelType implements INettySocketChannelType {

    public static final IOUringNettySocketChannelType INSTANCE = new IOUringNettySocketChannelType();

    @Override
    public EventLoopGroup newServerAcceptorGroup() {
        return new IOUringEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return new IOUringEventLoopGroup(1);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return new IOUringEventLoopGroup(1);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return IOUringServerSocketChannel.class;
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return IOUringSocketChannel.class;
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        NioNettySocketChannelType.INSTANCE.channelOptions(consumer, socketSize);
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        NioNettySocketChannelType.INSTANCE.initChannel(channel, server);
    }

}