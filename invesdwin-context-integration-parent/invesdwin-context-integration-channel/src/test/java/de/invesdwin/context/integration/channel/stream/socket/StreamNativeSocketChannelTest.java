package de.invesdwin.context.integration.channel.stream.socket;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.stream.StreamLatencyChannelTest;
import de.invesdwin.context.integration.channel.stream.StreamThroughputChannelTest;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannelServer;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketEndpointFactory;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class StreamNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testStreamLatency() throws InterruptedException {
        final boolean lowLatency = true;
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession>(
                new SocketSynchronousChannelServer(address, getMaxMessageSize(), lowLatency)) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final SocketSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSocketSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSocketSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter), acceptedClientChannel);
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeSocketEndpointFactory(
                address, false, getMaxMessageSize(), lowLatency);
        new StreamLatencyChannelTest(this).runStreamLatencyTest(serverAcceptor, clientEndpointFactory);
    }

    @Test
    public void testStreamThroughput() throws InterruptedException {
        final boolean lowLatency = false;
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession>(
                new SocketSynchronousChannelServer(address, getMaxMessageSize(), lowLatency)) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final SocketSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSocketSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSocketSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter), acceptedClientChannel);
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeSocketEndpointFactory(
                address, false, getMaxMessageSize(), lowLatency);
        new StreamThroughputChannelTest(this) {
            //uncomment this to enable batching
            //            @Override
            //            public ISynchronousReader<IByteBufferProvider> newStreamSynchronousEndpointClientReader(
            //                    final StreamSynchronousEndpointClientChannel channel) {
            //                return new BatchSynchronousReader(super.newStreamSynchronousEndpointClientReader(channel));
            //            }
            //
            //            @Override
            //            public ISynchronousWriter<IByteBufferProvider> newStreamSynchronousEndpointClientWriter(
            //                    final StreamSynchronousEndpointClientChannel channel) {
            //                return new BatchSynchronousWriter(super.newStreamSynchronousEndpointClientWriter(channel), 1024 * 1024,
            //                        Integer.MAX_VALUE, Duration.ONE_MINUTE);
            //            }
        }.runStreamThroughputTest(serverAcceptor, clientEndpointFactory);
    }

}
