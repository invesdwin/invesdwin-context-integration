package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class MinaSocketEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final IMinaSocketType type;
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public MinaSocketEndpointFactory(final IMinaSocketType type, final InetSocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final MinaSocketSynchronousChannel clientChannel = newMinaSocketSynchronousChannel(type, address, server,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaSocketSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaSocketSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected MinaSocketSynchronousChannel newMinaSocketSynchronousChannel(final IMinaSocketType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new MinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}