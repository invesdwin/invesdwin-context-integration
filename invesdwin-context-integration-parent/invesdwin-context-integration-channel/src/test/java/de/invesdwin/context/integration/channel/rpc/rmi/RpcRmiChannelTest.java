package de.invesdwin.context.integration.channel.rpc.rmi;

import java.net.InetSocketAddress;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.async.IAsynchronousChannel;
import de.invesdwin.context.integration.channel.rpc.base.ARpcChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.transformer.DefaultSynchronousEndpointSessionFactoryTransformer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcRmiChannelTest extends ARpcChannelTest {

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
                    new ProcessedEventsRateString(VALUES * newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final InetSocketAddress address, final RpcTestServiceMode mode)
            throws InterruptedException {
        final Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel> serverFactory = new Function<AsynchronousEndpointServerHandlerFactory, IAsynchronousChannel>() {
            @Override
            public IAsynchronousChannel apply(final AsynchronousEndpointServerHandlerFactory t) {
                final RmiSynchronousEndpointServer channel = new RmiSynchronousEndpointServer(t,
                        new DefaultSynchronousEndpointSessionFactoryTransformer());
                return channel;
            }
        };
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new RmiSynchronousEndpointClientFactory();
        runRpcBlockingPerformanceTest(serverFactory, clientEndpointFactory, mode);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
