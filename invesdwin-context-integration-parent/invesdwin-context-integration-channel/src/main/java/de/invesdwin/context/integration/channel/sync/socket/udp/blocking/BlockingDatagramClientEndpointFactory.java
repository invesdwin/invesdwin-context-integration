package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class BlockingDatagramClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final SocketAddress address;
    private final int estimatedMaxMessageSize;

    public BlockingDatagramClientEndpointFactory(final SocketAddress address, final int estimatedMaxMessageSize) {
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final BlockingDatagramSynchronousChannel clientChannel = newDatagramSynchronousChannel(address, false,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingDatagramSynchronousReader(
                clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingDatagramSynchronousWriter(
                clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected BlockingDatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingDatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}