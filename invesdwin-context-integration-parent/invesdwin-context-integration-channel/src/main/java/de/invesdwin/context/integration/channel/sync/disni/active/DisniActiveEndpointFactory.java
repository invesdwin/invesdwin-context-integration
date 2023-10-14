package de.invesdwin.context.integration.channel.sync.disni.active;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class DisniActiveEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public DisniActiveEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final DisniActiveSynchronousChannel clientChannel = newDisniActiveSynchronousChannel(address, server,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new DisniActiveSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new DisniActiveSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected DisniActiveSynchronousChannel newDisniActiveSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DisniActiveSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}