package de.invesdwin.context.integration.channel.sync.netty.udp.nativee;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.EpollNettyDatagramChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NettyNativeDatagramChannelTest extends AChannelTest {

    @Test
    public void testNettyNativeDatagramChannelPerformance() throws InterruptedException {
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNettyNativeDatagramChannelPerformanceTest(EpollNettyDatagramChannelType.INSTANCE, responseAddress,
                requestAddress);
    }

    private void runNettyNativeDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettyNativeDatagramSynchronousWriter(type,
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new NettyNativeDatagramSynchronousReader(type,
                requestAddress, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors
                .newFixedThreadPool("runNettyNativeDatagramChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettyNativeDatagramSynchronousWriter(type,
                requestAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new NettyNativeDatagramSynchronousReader(type,
                responseAddress, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
