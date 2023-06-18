package de.invesdwin.context.integration.channel.rpc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.udt.NettyUdtAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.NioNettyUdtChannelType;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcNettyUdtHandlerTest extends AChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        System.out.println("TODO: bidi not supported");
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

    protected void runRpcTest(final InetSocketAddress address, final RpcTestServiceMode mode)
            throws InterruptedException {
        final INettyUdtChannelType type = NioNettyUdtChannelType.INSTANCE;
        final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final AsynchronousEndpointServerHandlerFactory t) {
                final NettyUdtSynchronousChannel channel = new NettyUdtSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                };
                return new NettyUdtAsynchronousChannel(channel, t, true);
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettyUdtClientEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newClientWorkerGroupThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        runRpcHandlerPerformanceTest(serverFactory, clientEndpointFactory, mode);
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
