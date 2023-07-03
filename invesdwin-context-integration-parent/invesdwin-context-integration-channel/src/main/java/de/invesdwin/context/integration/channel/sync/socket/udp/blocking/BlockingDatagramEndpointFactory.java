package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.server.async.IAsynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.async.IAsynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class BlockingDatagramEndpointFactory
        implements IAsynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, SocketAddress> {

    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public BlockingDatagramEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.address = address;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public IAsynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> newEndpoint() {
        final BlockingDatagramSynchronousChannel channel = newDatagramSynchronousChannel(address, server,
                estimatedMaxMessageSize);
        if (server) {
            channel.setMultipleClientsAllowed();
        }
        final ISynchronousReader<IByteBufferProvider> reader = new BlockingDatagramSynchronousReader(channel);
        final ISynchronousWriter<IByteBufferProvider> writer = new BlockingDatagramSynchronousWriter(channel);
        return new BlockingDatagramEndpoint(writer, channel, reader);
    }

    protected BlockingDatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingDatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    private static final class BlockingDatagramEndpoint
            implements IAsynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
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
        public void setOtherRemoteAddress(final SocketAddress otherRemoteAddress) {
            channel.setOtherSocketAddress(otherRemoteAddress);
        }

        @Override
        public SocketAddress getOtherRemoteAddress() {
            return channel.getOtherSocketAddress();
        }
    }
}