package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NettyDatagramSynchronousChannelTest extends AChannelTest {

    @Test
    public void testNettyDatagramChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettyDatagramChannelPerformanceTest(INettyDatagramChannelType.getDefault(), responseAddress, requestAddress);
    }

    private void runNettyDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyDatagramSynchronousWriter(type,
                responseAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> requestReader = new NettyDatagramSynchronousReader(type, requestAddress,
                getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettyDatagramChannelPerformanceTest",
                1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyDatagramSynchronousWriter(type,
                requestAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> responseReader = new NettyDatagramSynchronousReader(type, responseAddress,
                getMaxMessageSize());
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
