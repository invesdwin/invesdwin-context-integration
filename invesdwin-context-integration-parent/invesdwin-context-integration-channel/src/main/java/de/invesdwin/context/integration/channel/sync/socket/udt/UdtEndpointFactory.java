package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class UdtEndpointFactory implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public UdtEndpointFactory(final InetSocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final UdtSynchronousChannel clientChannel = newUdtSynchronousChannel(address, server, estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new UdtSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new UdtSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected UdtSynchronousChannel newUdtSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new UdtSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}