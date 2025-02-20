package de.invesdwin.context.integration.channel.rpc.rdma;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.rpc.base.ARpcLatencyChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.disni.active.DisniActiveEndpointFactory;
import de.invesdwin.context.integration.channel.sync.disni.active.DisniActiveSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.disni.active.DisniActiveSynchronousChannelServer;
import de.invesdwin.context.integration.channel.sync.disni.active.DisniActiveSynchronousReader;
import de.invesdwin.context.integration.channel.sync.disni.active.DisniActiveSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcSynchronousDisniActiveChannelTest extends ARpcLatencyChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runRpcTest(address, RpcTestServiceMode.requestFalseTrue);
    }

    @Test
    public void testRpcAllModes() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        for (final RpcTestServiceMode mode : RpcTestServiceMode.values()) {
            final InetSocketAddress address = new InetSocketAddress(addr, port);
            log.warn("%s.%s: Starting", RpcTestServiceMode.class.getSimpleName(), mode);
            final Instant start = new Instant();
            runRpcTest(address, mode);
            final Duration duration = start.toDuration();
            log.warn("%s.%s: Finished after %s with %s (with connection establishment)",
                    RpcTestServiceMode.class.getSimpleName(), mode, duration,
                    new ProcessedEventsRateString(MESSAGE_COUNT * newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final SocketAddress address, final RpcTestServiceMode mode) throws InterruptedException {
        final ATransformingSynchronousReader<DisniActiveSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<DisniActiveSynchronousChannel, ISynchronousEndpointSession>(
                new DisniActiveSynchronousChannelServer(address, getMaxMessageSize())) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final DisniActiveSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new DisniActiveSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new DisniActiveSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter), acceptedClientChannel);
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new DisniActiveEndpointFactory(
                address, false, getMaxMessageSize());
        runRpcPerformanceTest(serverAcceptor, clientEndpointFactory, mode);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
