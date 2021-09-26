package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.EpollNettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NettySocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNettySocketChannelPerformanceTest(EpollNettySocketChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final SocketAddress responseAddress, final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettySocketSynchronousWriter(type,
                responseAddress, true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new NettySocketSynchronousReader(type, requestAddress,
                false, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettySocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettySocketSynchronousWriter(type,
                requestAddress, true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new NettySocketSynchronousReader(type, responseAddress,
                false, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
