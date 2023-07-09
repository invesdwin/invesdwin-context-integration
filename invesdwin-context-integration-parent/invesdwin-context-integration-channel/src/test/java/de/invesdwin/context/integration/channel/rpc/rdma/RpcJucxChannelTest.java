package de.invesdwin.context.integration.channel.rpc.rdma;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.rpc.ARpcChannelTest;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.ImmutableSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.DefaultSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.ATransformingSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.JucxEndpointFactory;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousChannelServer;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousReader;
import de.invesdwin.context.integration.channel.sync.jucx.JucxSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.context.integration.channel.sync.jucx.type.JucxTransportType;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcJucxChannelTest extends ARpcChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runRpcTest(newType(), address, RpcTestServiceMode.requestFalseTrue);
    }

    private JucxTransportType newType() {
        return JucxTransportType.STREAM;
    }

    @Test
    public void testRpcAllModes() throws InterruptedException {
        for (final RpcTestServiceMode mode : RpcTestServiceMode.values()) {
            final String addr = findLocalNetworkAddress();
            final int port = NetworkUtil.findAvailableTcpPort();
            final InetSocketAddress address = new InetSocketAddress(addr, port);
            log.warn("%s.%s: Starting", RpcTestServiceMode.class.getSimpleName(), mode);
            final Instant start = new Instant();
            runRpcTest(newType(), address, mode);
            final Duration duration = start.toDuration();
            log.warn("%s.%s: Finished after %s with %s (with connection establishment)",
                    RpcTestServiceMode.class.getSimpleName(), mode, duration,
                    new ProcessedEventsRateString(VALUES * newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final IJucxTransportType type, final InetSocketAddress address,
            final RpcTestServiceMode mode) throws InterruptedException {
        final ATransformingSynchronousReader<JucxSynchronousChannel, ISynchronousEndpointSession> serverAcceptor = new ATransformingSynchronousReader<JucxSynchronousChannel, ISynchronousEndpointSession>(
                new JucxSynchronousChannelServer(type, address, getMaxMessageSize())) {
            private final AtomicInteger index = new AtomicInteger();

            @Override
            protected ISynchronousEndpointSession transform(final JucxSynchronousChannel acceptedClientChannel) {
                final ISynchronousReader<IByteBufferProvider> requestReader = new JucxSynchronousReader(
                        acceptedClientChannel);
                final ISynchronousWriter<IByteBufferProvider> responseWriter = new JucxSynchronousWriter(
                        acceptedClientChannel);
                return new DefaultSynchronousEndpointSession(String.valueOf(index.incrementAndGet()),
                        ImmutableSynchronousEndpoint.of(requestReader, responseWriter));
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new JucxEndpointFactory(
                type, address, false, getMaxMessageSize());
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
