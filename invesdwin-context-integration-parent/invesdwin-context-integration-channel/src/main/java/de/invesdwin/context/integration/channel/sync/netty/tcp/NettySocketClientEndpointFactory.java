package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NettySocketClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final INettySocketChannelType type;
    private final InetSocketAddress address;
    private final int estimatedMaxMessageSize;

    public NettySocketClientEndpointFactory(final INettySocketChannelType type, final InetSocketAddress address,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettySocketSynchronousChannel clientChannel = newNettySocketSynchronousChannel(type, address, false,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettySocketSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettySocketSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected NettySocketSynchronousChannel newNettySocketSynchronousChannel(final INettySocketChannelType type2,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}