package de.invesdwin.context.integration.channel.netty.type;

import javax.annotation.concurrent.Immutable;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

@Immutable
public class SslDelegateNettyChannelType implements INettyChannelType {

    private final INettyChannelType delegate;
    private final SslContext sslContext;

    public SslDelegateNettyChannelType(final INettyChannelType delegate, final SslContext sslContext) {
        this.delegate = delegate;
        this.sslContext = sslContext;
    }

    @Override
    public EventLoopGroup newServerBossGroup() {
        return delegate.newServerBossGroup();
    }

    @Override
    public EventLoopGroup newServerWorkerGroup(final EventLoopGroup bossGroup) {
        return delegate.newServerWorkerGroup(bossGroup);
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelType() {
        return delegate.getServerChannelType();
    }

    @Override
    public Class<? extends Channel> getClientChannelType() {
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
