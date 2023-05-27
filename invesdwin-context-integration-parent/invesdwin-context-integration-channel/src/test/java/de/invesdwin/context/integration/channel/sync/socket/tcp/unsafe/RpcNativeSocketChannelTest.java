package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannelServer;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class RpcNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNioSocketRpcPerformanceTest(address);
    }

    protected void runNioSocketRpcPerformanceTest(final SocketAddress address) throws InterruptedException {
        final ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<SocketSynchronousChannel, ISynchronousEndpointSession>(
                new SocketSynchronousChannelServer(address, getMaxMessageSize())) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final SocketSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSocketSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSocketSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter));
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider>() {
            @Override
            public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
                final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false,
                        getMaxMessageSize());
                final ISynchronousReader<IByteBufferProvider> responseReader = new NativeSocketSynchronousReader(
                        clientChannel);
                final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeSocketSynchronousWriter(
                        clientChannel);
                return ImmutableSynchronousEndpoint.of(responseReader, requestWriter);
            }
        };
        runRpcPerformanceTest(serverAcceptor, clientEndpointFactory);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
