package de.invesdwin.context.integration.channel.rpc.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.netty.tcp.NettySocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.ARpcChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySharedSocketClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcNettySocketHandlerTest extends ARpcChannelTest {

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
                    new ProcessedEventsRateString(MESSAGE_COUNT * newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final InetSocketAddress address, final RpcTestServiceMode mode)
            throws InterruptedException {
        final INettySocketChannelType type = INettySocketChannelType.getDefault();
        final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final AsynchronousEndpointServerHandlerFactory t) {
                final NettySocketSynchronousChannel channel = new NettySocketSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newServerWorkerGroupThreadCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                };
                return new NettySocketAsynchronousChannel(channel, t, true);
            }
        };
        //netty shared bootstrap
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySharedSocketClientEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newClientWorkerGroupThreadCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        //netty no shared bootstrap
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NettySocketClientEndpointFactory(
        //                type, address, getMaxMessageSize());
        //fastest
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeSocketClientEndpointFactory(
        //                address, getMaxMessageSize());
        runRpcHandlerPerformanceTest(serverFactory, clientEndpointFactory, mode);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
