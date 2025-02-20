package de.invesdwin.context.integration.channel.rpc.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.udt.NettyUdtAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.RpcLatencyChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettySharedUdtClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.netty.udt.NettyUdtSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.NioNettyUdtChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcNettyUdtHandlerTest extends AChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        final RpcLatencyChannelTest test = new RpcLatencyChannelTest(this);
        runRpcTest(test, address, RpcTestServiceMode.requestFalseTrue);
    }

    @Disabled("udt over netty is too slow")
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

    protected void runRpcTest(final RpcLatencyChannelTest test, final InetSocketAddress address,
            final RpcTestServiceMode mode) throws InterruptedException {
        final INettyUdtChannelType type = NioNettyUdtChannelType.INSTANCE;
        final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final RpcAsynchronousEndpointServerHandlerFactory t) {
                final NettyUdtSynchronousChannel channel = new NettyUdtSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return RpcLatencyChannelTest.RPC_CLIENT_TRANSPORTS;
                    }
                };
                return new NettyUdtAsynchronousChannel(channel, t, true);
            }
        };
        //netty shared bootstrap
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySharedUdtClientEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newClientWorkerGroupThreadCount() {
                return RpcLatencyChannelTest.RPC_CLIENT_TRANSPORTS;
            }
        };
        //netty no shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettyUdtClientEndpointFactory(
        //                type, address, getMaxMessageSize());
        test.runRpcHandlerPerformanceTest(serverFactory, clientEndpointFactory, mode);
    }

    @Override
    public int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
