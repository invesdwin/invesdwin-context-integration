package de.invesdwin.context.integration.channel.rpc.socket;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.mina.MinaSocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.RpcLatencyChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.mina.MinaSharedSocketEndpointFactory;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcMinaDatagramHandlerTest extends AChannelTest {

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

    protected void runRpcTest(final RpcLatencyChannelTest test, final InetSocketAddress address,
            final RpcTestServiceMode mode) throws InterruptedException {
        final MinaSocketType type = MinaSocketType.NioUdp;
        final Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<RpcAsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final RpcAsynchronousEndpointServerHandlerFactory t) {
                final MinaSocketSynchronousChannel channel = new MinaSocketSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newAcceptorProcessorCount() {
                        return RpcLatencyChannelTest.RPC_CLIENT_TRANSPORTS;
                    }
                };
                return new MinaSocketAsynchronousChannel(channel, t, true);
            }
        };
        //mina shared clientConnector
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new MinaSharedSocketEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newConnectorProcessorCount() {
                return RpcLatencyChannelTest.RPC_CLIENT_TRANSPORTS;
            }
        };
        //mina no shared clientConnector
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new MinaSocketClientEndpointFactory(
        //                type, address, getMaxMessageSize());
        //fastest
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new NativeDatagramClientEndpointFactory(
        //                address, getMaxMessageSize());
        test.runRpcHandlerPerformanceTest(serverFactory, clientEndpointFactory, mode);
    }

    @Override
    public int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
