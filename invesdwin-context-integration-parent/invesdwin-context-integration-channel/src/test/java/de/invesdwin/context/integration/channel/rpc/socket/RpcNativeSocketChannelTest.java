package de.invesdwin.context.integration.channel.rpc.socket;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.RpcLatencyChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannelServer;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketEndpointFactory;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final RpcLatencyChannelTest test = new RpcLatencyChannelTest(this);
        runRpcTest(test, address, RpcTestServiceMode.requestFalseTrue);
    }

    @Test
    public void testRpcAllModes() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final RpcLatencyChannelTest test = new RpcLatencyChannelTest(this);
        for (final RpcTestServiceMode mode : RpcTestServiceMode.values()) {
            log.warn("%s.%s: Starting", RpcTestServiceMode.class.getSimpleName(), mode);
            final Instant start = new Instant();
            runRpcTest(test, address, mode);
            final Duration duration = start.toDuration();
            log.warn("%s.%s: Finished after %s with %s (with connection establishment)",
                    RpcTestServiceMode.class.getSimpleName(), mode, duration,
                    new ProcessedEventsRateString(MESSAGE_COUNT * test.newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final RpcLatencyChannelTest test, final SocketAddress address,
            final RpcTestServiceMode mode) throws InterruptedException {
        final boolean lowLatency = true;
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
        test.runRpcPerformanceTest(serverAcceptor, clientEndpointFactory, mode);
    }

}
