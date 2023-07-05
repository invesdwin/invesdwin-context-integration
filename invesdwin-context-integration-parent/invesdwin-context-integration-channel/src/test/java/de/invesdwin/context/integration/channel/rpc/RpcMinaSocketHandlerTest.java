package de.invesdwin.context.integration.channel.rpc;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.async.mina.MinaSocketAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.mina.MinaSharedSocketClientEndpointFactory;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcMinaSocketHandlerTest extends ARpcChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runRpcTest(address, RpcTestServiceMode.requestFalseTrue);
    }

    @Disabled("has issues with closing")
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
        final MinaSocketType type = MinaSocketType.NioTcp;
        final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final AsynchronousEndpointServerHandlerFactory t) {
                final MinaSocketSynchronousChannel channel = new MinaSocketSynchronousChannel(type, address, true,
                        getMaxMessageSize()) {
                    @Override
                    protected int newAcceptorProcessorCount() {
                        return RPC_CLIENT_TRANSPORTS;
                    }
                };
                return new MinaSocketAsynchronousChannel(channel, t, true);
            }
        };
        //mina shared clientConnector
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new MinaSharedSocketClientEndpointFactory(
                type, address, getMaxMessageSize()) {
            @Override
            protected int newConnectorProcessorCount() {
                return RPC_CLIENT_TRANSPORTS;
            }
        };
        //mina no shared clientConnector
        //        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new MinaSocketClientEndpointFactory(
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
