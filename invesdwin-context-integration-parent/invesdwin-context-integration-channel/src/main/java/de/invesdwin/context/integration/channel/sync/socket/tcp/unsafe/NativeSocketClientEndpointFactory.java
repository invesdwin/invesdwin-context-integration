package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NativeSocketClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final SocketAddress address;
    private final int estimatedMaxMessageSize;

    public NativeSocketClientEndpointFactory(final SocketAddress address, final int estimatedMaxMessageSize) {
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeSocketSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeSocketSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}