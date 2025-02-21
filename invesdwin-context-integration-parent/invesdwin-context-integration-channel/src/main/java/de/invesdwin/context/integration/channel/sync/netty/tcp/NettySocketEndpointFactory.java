package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
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
    private final boolean lowLatency;

    public NettySocketEndpointFactory(final INettySocketChannelType type, final InetSocketAddress address,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.type = type;
        this.server = server;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.lowLatency = lowLatency;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettySocketSynchronousChannel clientChannel = newNettySocketSynchronousChannel(type, address, server,
                estimatedMaxMessageSize, lowLatency);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettySocketSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettySocketSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected NettySocketSynchronousChannel newNettySocketSynchronousChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }
}