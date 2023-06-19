package de.invesdwin.context.integration.channel.sync.netty.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.INettyUdtChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udt.type.NioNettyUdtChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NettyUdtSynchronousChannelTest extends AChannelTest {

    @Test
    public void testNettyUdtChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettyUdtChannelPerformanceTest(NioNettyUdtChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettyUdtChannelPerformanceTest(final INettyUdtChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyUdtSynchronousWriter(type,
                responseAddress, false, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettyUdtSynchronousReader(type,
                requestAddress, true, getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettyDatagramChannelPerformanceTest",
                1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyUdtSynchronousWriter(type,
                requestAddress, false, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyUdtSynchronousReader(type,
                responseAddress, true, getMaxMessageSize());
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
