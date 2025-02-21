package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NettyDatagramEndpointFactory
        implements ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, InetSocketAddress> {
    private final INettyDatagramChannelType type;
    private final InetSocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;
    private final boolean lowLatency;

    public NettyDatagramEndpointFactory(final INettyDatagramChannelType type, final InetSocketAddress address,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.type = type;
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.lowLatency = lowLatency;
    }

    @Override
    public ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, InetSocketAddress> newEndpoint() {
        final NettyDatagramSynchronousChannel channel = newNettyDatagramSynchronousChannel(type, address, server,
                estimatedMaxMessageSize, lowLatency);
        if (server) {
            channel.setMultipleClientsAllowed();
        }
        final ISynchronousReader<IByteBufferProvider> reader = new NettyDatagramSynchronousReader(channel);
        final ISynchronousWriter<IByteBufferProvider> writer = new NettyDatagramSynchronousWriter(channel);
        return new NettyDatagramEndpoint(channel, writer, reader);
    }

    protected NettyDatagramSynchronousChannel newNettyDatagramSynchronousChannel(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new NettyDatagramSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    private static final class NettyDatagramEndpoint
            implements ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, InetSocketAddress> {
        private final NettyDatagramSynchronousChannel channel;
        private final ISynchronousWriter<IByteBufferProvider> writer;
        private final ISynchronousReader<IByteBufferProvider> reader;

        private NettyDatagramEndpoint(final NettyDatagramSynchronousChannel channel,
                final ISynchronousWriter<IByteBufferProvider> writer,
                final ISynchronousReader<IByteBufferProvider> reader) {
            this.channel = channel;
            this.writer = writer;
            this.reader = reader;
        }

        @Override
        public ISynchronousWriter<IByteBufferProvider> getWriter() {
            return writer;
        }

        @Override
        public ISynchronousReader<IByteBufferProvider> getReader() {
            return reader;
        }

        @Override
        public void setOtherSocketAddress(final InetSocketAddress otherSocketAddress) {
            channel.setOtherSocketAddress(otherSocketAddress);
        }

        @Override
        public InetSocketAddress getOtherSocketAddress() {
            return channel.getOtherSocketAddress();
        }
    }
}