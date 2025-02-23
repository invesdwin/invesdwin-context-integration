package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NativeSocketEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;
    private final boolean lowLatency;

    public NativeSocketEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.lowLatency = lowLatency;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, server,
                estimatedMaxMessageSize, lowLatency);
        final ISynchronousReader<IByteBufferProvider> responseReader = newReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected ISynchronousWriter<IByteBufferProvider> newWriter(final SocketSynchronousChannel clientChannel) {
        return new NativeSocketSynchronousWriter(clientChannel);
    }

    protected ISynchronousReader<IByteBufferProvider> newReader(final SocketSynchronousChannel clientChannel) {
        return new NativeSocketSynchronousReader(clientChannel);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }
}