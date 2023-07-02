package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NativeDatagramClientEndpointFactory
        implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final SocketAddress address;
    private final int estimatedMaxMessageSize;

    public NativeDatagramClientEndpointFactory(final SocketAddress address, final int estimatedMaxMessageSize) {
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final DatagramSynchronousChannel clientChannel = newDatagramSynchronousChannel(address, false,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeDatagramSynchronousReader(
                clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeDatagramSynchronousWriter(
                clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}