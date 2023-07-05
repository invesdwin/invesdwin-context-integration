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
public class NettySocketEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final INettySocketChannelType type;
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public NettySocketEndpointFactory(final INettySocketChannelType type, final InetSocketAddress address,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.server = server;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettySocketSynchronousChannel clientChannel = newNettySocketSynchronousChannel(type, address, server,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettySocketSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettySocketSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected NettySocketSynchronousChannel newNettySocketSynchronousChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}