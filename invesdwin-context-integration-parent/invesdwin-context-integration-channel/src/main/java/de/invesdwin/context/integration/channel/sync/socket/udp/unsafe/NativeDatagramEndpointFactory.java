package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.net.SocketAddress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.rpc.server.sessionless.ISessionlessSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Immutable
public class NativeDatagramEndpointFactory
        implements ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
    private final SocketAddress address;
    private final boolean server;
    private final int estimatedMaxMessageSize;

    public NativeDatagramEndpointFactory(final SocketAddress address, final boolean server,
            final int estimatedMaxMessageSize) {
        this.server = server;
        this.address = address;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
    }

    @Override
    public ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> newEndpoint() {
        final DatagramSynchronousChannel channel = newDatagramSynchronousChannel(address, server,
                estimatedMaxMessageSize);
        if (server) {
            channel.setMultipleClientsAllowed();
        }
        final ISynchronousReader<IByteBufferProvider> reader = new NativeDatagramSynchronousReader(channel);
        final ISynchronousWriter<IByteBufferProvider> writer = new NativeDatagramSynchronousWriter(channel);
        return new NativeDatagramEndpoint(writer, channel, reader);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    private static final class NativeDatagramEndpoint
            implements ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, SocketAddress> {
        private final ISynchronousWriter<IByteBufferProvider> writer;
        private final DatagramSynchronousChannel channel;
        private final ISynchronousReader<IByteBufferProvider> reader;

        private NativeDatagramEndpoint(final ISynchronousWriter<IByteBufferProvider> writer,
                final DatagramSynchronousChannel channel, final ISynchronousReader<IByteBufferProvider> reader) {
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