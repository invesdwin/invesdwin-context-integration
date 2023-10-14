package de.invesdwin.context.integration.channel.sync.netty.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NettyUdtEndpointFactory implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final INettyUdtChannelType type;
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public NettyUdtEndpointFactory(final INettyUdtChannelType type, final InetSocketAddress address,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.server = server;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettyUdtSynchronousChannel clientChannel = newNettyUdtSynchronousChannel(type, address, server,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyUdtSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyUdtSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected NettyUdtSynchronousChannel newNettyUdtSynchronousChannel(final INettyUdtChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettyUdtSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}