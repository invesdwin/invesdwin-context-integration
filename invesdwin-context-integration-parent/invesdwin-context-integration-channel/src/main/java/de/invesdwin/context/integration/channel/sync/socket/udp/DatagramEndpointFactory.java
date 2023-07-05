package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.endpoint.sessionless.ISessionlessSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class DatagramEndpointFactory
        implements ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public DatagramEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> newEndpoint() {
        final DatagramSynchronousChannel channel = newDatagramSynchronousChannel(address, server,
                estimatedMaxMessageSize);
        if (server) {
            channel.setMultipleClientsAllowed();
        }
        final ISynchronousReader<IByteBufferProvider> reader = new DatagramSynchronousReader(channel);
        final ISynchronousWriter<IByteBufferProvider> writer = new DatagramSynchronousWriter(channel);
        return new DatagramEndpoint(channel, reader, writer);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    private static final class DatagramEndpoint
            implements ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
        private final DatagramSynchronousChannel channel;
        private final ISynchronousReader<IByteBufferProvider> reader;
        private final ISynchronousWriter<IByteBufferProvider> writer;

        private DatagramEndpoint(final DatagramSynchronousChannel channel,
                final ISynchronousReader<IByteBufferProvider> reader,
                final ISynchronousWriter<IByteBufferProvider> writer) {
            this.channel = channel;
            this.reader = reader;
            this.writer = writer;
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