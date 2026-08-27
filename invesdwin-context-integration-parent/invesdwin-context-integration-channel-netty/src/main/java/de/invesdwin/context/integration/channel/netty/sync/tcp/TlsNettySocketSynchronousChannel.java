package de.invesdwin.context.integration.channel.netty.sync.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.SSLEngine;

import de.invesdwin.context.integration.channel.netty.sync.crypto.NettyDerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.netty.sync.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.provider.ITransportLayerSecurityProvider;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

@Immutable
public class TlsNettySocketSynchronousChannel extends NettySocketSynchronousChannel {

    public TlsNettySocketSynchronousChannel(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        super(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
        //unsafe write not supported, this would circumvent the ssl handler
        setKeepBootstrapRunningAfterOpen();
    }

    @Override
    protected void onSocketChannel(final SocketChannel socketChannel) {
        final ChannelPipeline pipeline = socketChannel.pipeline();

        final ITransportLayerSecurityProvider tlsProvider = newTransportLayerSecurityProvider(socketChannel.alloc());
        final SSLEngine engine = tlsProvider.newEngine();

        final SslHandler sslHandler = new SslHandler(engine, tlsProvider.isStartTlsEnabled());
        pipeline.addLast(sslHandler);

        super.onSocketChannel(socketChannel);
    }

    protected ITransportLayerSecurityProvider newTransportLayerSecurityProvider(final ByteBufAllocator alloc) {
        return new NettyDerivedKeyTransportLayerSecurityProvider(socketAddress, server) {
            @Override
            protected ByteBufAllocator getByteBufAllocator() {
                return alloc;
            }
        };
    }

}
