package de.invesdwin.context.integration.channel.sync.jucx;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class JucxEndpointFactory implements ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> {
    private final IJucxTransportType type;
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public JucxEndpointFactory(final IJucxTransportType type, final InetSocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
        final JucxSynchronousChannel clientChannel = newJucxSynchronousChannel(type, address, server,
                estimatedMaxMessageSize);
        final ISynchronousReader<IByteBufferProvider> responseReader = new JucxSynchronousReader(clientChannel);
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new JucxSynchronousWriter(clientChannel);
        return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
    }

    protected JucxSynchronousChannel newJucxSynchronousChannel(final IJucxTransportType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new JucxSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }
}