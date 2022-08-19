package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.channel.NettySocketChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.EpollNettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NettyNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final int responsePort = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", responsePort);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", responsePort + 1);
        runNettySocketChannelPerformanceTest(EpollNettySocketChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettyNativeSocketSynchronousWriter(
                new NettySocketChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> requestReader = new NettyNativeSocketSynchronousReader(
                new NettySocketChannel(type, requestAddress, false, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettySocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettyNativeSocketSynchronousWriter(
                new NettySocketChannel(type, requestAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> responseReader = new NettyNativeSocketSynchronousReader(
                new NettySocketChannel(type, responseAddress, false, getMaxMessageSize()));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
