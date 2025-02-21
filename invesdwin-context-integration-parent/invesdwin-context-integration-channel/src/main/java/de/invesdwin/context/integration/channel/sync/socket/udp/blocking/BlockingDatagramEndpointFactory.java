package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class BlockingDatagramEndpointFactory
        implements ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, SocketAddress> {

    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;
    private final boolean lowLatency;

    public BlockingDatagramEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize, final boolean lowLatency) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.lowLatency = lowLatency;
    }

    @Override
    public ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> newEndpoint() {
        final BlockingDatagramSynchronousChannel channel = newDatagramSynchronousChannel(address, server,
                estimatedMaxMessageSize, lowLatency);
        if (server) {
            channel.setMultipleClientsAllowed();
        }
        final ISynchronousReader<IByteBufferProvider> reader = new BlockingDatagramSynchronousReader(channel);
        final ISynchronousWriter<IByteBufferProvider> writer = new BlockingDatagramSynchronousWriter(channel);
        return new BlockingDatagramEndpoint(writer, channel, reader);
    }

    protected BlockingDatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new BlockingDatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    private static final class BlockingDatagramEndpoint
            implements ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
        private final ISynchronousWriter<IByteBufferProvider> writer;
        private final BlockingDatagramSynchronousChannel channel;
        private final ISynchronousReader<IByteBufferProvider> reader;

        private BlockingDatagramEndpoint(final ISynchronousWriter<IByteBufferProvider> writer,
                final BlockingDatagramSynchronousChannel channel,
                final ISynchronousReader<IByteBufferProvider> reader) {
            this.writer = writer;
            this.channel = channel;
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
        public void setOtherSocketAddress(final SocketAddress otherSocketAddress) {
            channel.setOtherSocketAddress(otherSocketAddress);
        }

        @Override
        public SocketAddress getOtherSocketAddress() {
            return channel.getOtherSocketAddress();
        }
    }
}