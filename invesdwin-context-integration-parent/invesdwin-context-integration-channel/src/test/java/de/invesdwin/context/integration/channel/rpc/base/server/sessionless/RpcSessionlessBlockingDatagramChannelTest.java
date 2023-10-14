package de.invesdwin.context.integration.channel.rpc.base.server.sessionless;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.rpc.base.ARpcChannelTest;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcTestServiceMode;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramEndpointFactory;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.string.ProcessedEventsRateString;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class RpcSessionlessBlockingDatagramChannelTest extends ARpcChannelTest {

    @Test
    public void testRpcPerformance() throws InterruptedException {
        Throwables.setDebugStackTraceEnabled(true);
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runRpcTest(address, RpcTestServiceMode.requestFalseTrue);
    }

    @Disabled("can deadlock during close")
    @Test
    public void testRpcAllModes() throws InterruptedException {
        for (final RpcTestServiceMode mode : RpcTestServiceMode.values()) {
            final int port = NetworkUtil.findAvailableTcpPort();
            final InetSocketAddress address = new InetSocketAddress("localhost", port);
            log.warn("%s.%s: Starting", RpcTestServiceMode.class.getSimpleName(), mode);
            final Instant start = new Instant();
            runRpcTest(address, mode);
            final Duration duration = start.toDuration();
            log.warn("%s.%s: Finished after %s with %s (with connection establishment)",
                    RpcTestServiceMode.class.getSimpleName(), mode, duration,
                    new ProcessedEventsRateString(VALUES * newRpcClientThreads(), duration));
        }
    }

    protected void runRpcTest(final SocketAddress address, final RpcTestServiceMode mode) throws InterruptedException {
        final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory = new BlockingDatagramEndpointFactory(
                address, true, getMaxMessageSize());
        final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> clientEndpointFactory = new BlockingDatagramEndpointFactory(
                address, false, getMaxMessageSize());
        runRpcSessionlessPerformanceTest(serverEndpointFactory, clientEndpointFactory, mode);
    }

    @Override
    protected int getMaxMessageSize() {
        return super.getMaxMessageSize() + ServiceSynchronousCommandSerde.MESSAGE_INDEX;
    }

}
