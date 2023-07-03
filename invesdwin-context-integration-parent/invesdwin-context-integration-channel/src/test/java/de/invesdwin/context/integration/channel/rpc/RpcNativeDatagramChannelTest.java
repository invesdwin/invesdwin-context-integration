package de.invesdwin.context.integration.channel.rpc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannelServer;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.udp.unsafe.NativeDatagramSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcNativeDatagramChannelTest extends AChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runRpcTest(address, RpcTestServiceMode.requestFalseTrue);
    }

    @Test
    public void testRpcAllModes() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        for (final RpcTestServiceMode mode : RpcTestServiceMode.values()) {
            log.warn("%s.%s: Starting", RpcTestServiceMode.class.getSimpleName(), mode);
            final Instant start = new Instant();
            runRpcTest(address, mode);
            final Duration duration = start.toDuration();
            log.warn("%s.%s: Finished after %s with %s (with connection establishment)",
                    RpcTestServiceMode.class.getSimpleName(), mode, duration,
                    new ProcessedEventsRateString(VALUES * RPC_CLIENT_THREADS, duration));
        }
    }

    protected void runRpcTest(final SocketAddress address, final RpcTestServiceMode mode) throws InterruptedException {
        final ATransformingSynchronousReader<DatagramSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<DatagramSynchronousChannel, ISynchronousEndpointSession>(
                new DatagramSynchronousChannelServer(address, getMaxMessageSize())) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final DatagramSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new NativeDatagramSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeDatagramSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter));
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeDatagramClientEndpointFactory(
                address, getMaxMessageSize());
        runRpcPerformanceTest(serverAcceptor, clientEndpointFactory, mode);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
