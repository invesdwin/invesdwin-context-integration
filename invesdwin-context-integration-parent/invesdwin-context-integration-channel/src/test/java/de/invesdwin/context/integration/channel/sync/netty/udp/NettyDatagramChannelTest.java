package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.NioNettyDatagramChannelType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class NettyDatagramChannelTest extends AChannelTest {

    @Test
    public void testNettyDatagramChannelPerformance() throws InterruptedException {
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNettyDatagramChannelPerformanceTest(NioNettyDatagramChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettyDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettyDatagramSynchronousWriter(type,
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new NettyDatagramSynchronousReader(type, requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettyDatagramChannelPerformanceTest",
                1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettyDatagramSynchronousWriter(type,
                requestAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new NettyDatagramSynchronousReader(type, responseAddress,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
