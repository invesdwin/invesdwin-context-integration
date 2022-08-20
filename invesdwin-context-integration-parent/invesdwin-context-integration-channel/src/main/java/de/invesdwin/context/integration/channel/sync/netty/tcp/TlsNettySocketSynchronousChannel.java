package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;
import javax.net.ssl.SSLEngine;

import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.DerivedKeyTransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.crypto.handshake.provider.tls.ITransportLayerSecurityProvider;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

@Immutable
public class TlsNettySocketSynchronousChannel extends NettySocketSynchronousChannel {

    public TlsNettySocketSynchronousChannel(final INettySocketChannelType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        super(type, socketAddress, server, estimatedMaxMessageSize);
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
        return new DerivedKeyTransportLayerSecurityProvider(socketAddress, server) {
            @Override
            protected ByteBufAllocator getByteBufAllocator() {
                return alloc;
            }
        };
    }

}
