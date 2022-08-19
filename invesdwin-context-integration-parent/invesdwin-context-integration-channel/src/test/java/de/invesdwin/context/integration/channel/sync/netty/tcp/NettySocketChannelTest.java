package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.NioNettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NettySocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettySocketChannelPerformanceTest(NioNettySocketChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettySocketSynchronousWriter(
                newNettySocketChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> requestReader = new NettySocketSynchronousReader(
                newNettySocketChannel(type, requestAddress, false, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettySocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettySocketSynchronousWriter(
                newNettySocketChannel(type, requestAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> responseReader = new NettySocketSynchronousReader(
                newNettySocketChannel(type, responseAddress, false, getMaxMessageSize()));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    protected NettySocketChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
