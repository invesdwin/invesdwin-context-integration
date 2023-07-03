package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NettyDatagramClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final INettyDatagramChannelType type;
    private final InetSocketAddress address;
    private final int estimatedMaxMessageSize;

    public NettyDatagramClientEndpointFactory(final INettyDatagramChannelType type, final InetSocketAddress address,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final NettyDatagramSynchronousChannel clientChannel = newNettyDatagramSynchronousChannel(type, address, false,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyDatagramSynchronousReader(
                clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyDatagramSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected NettyDatagramSynchronousChannel newNettyDatagramSynchronousChannel(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettyDatagramSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}