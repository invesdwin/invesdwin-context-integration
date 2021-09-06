package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.netty.IChannelOptionConsumer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

@Immutable
public class SslDelegateNettySocketChannelType implements INettySocketChannelType {

    private final INettySocketChannelType delegate;
    private final SslContext sslContext;

    public SslDelegateNettySocketChannelType(final INettySocketChannelType delegate, final SslContext sslContext) {
        this.delegate = delegate;
        this.sslContext = sslContext;
    }

    @Override
    public EventLoopGroup newServerAcceptorGroup() {
        return delegate.newServerAcceptorGroup();
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return delegate.newServerWorkerGroup(bossGroup);
    }

    @Override
    public Class<? extends ServerSocketChannel> getServerChannelType() {
        return delegate.getServerChannelType();
    }

    @Override
    public Class<? extends SocketChannel> getClientChannelType() {
        return delegate.getClientChannelType();
    }

    @Override
    public void channelOptions(final IChannelOptionConsumer consumer, final int socketSize) {
        delegate.channelOptions(consumer, socketSize);
    }

    @Override
    public EventLoopGroup newClientWorkerGroup() {
        return delegate.newClientWorkerGroup();
    }

    @Override
    public void initChannel(final SocketChannel channel, final boolean server) throws Exception {
        channel.pipeline().addLast(sslContext.newHandler(channel.alloc()));
    }

}
