package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.IOUringNettySocketChannelType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class NettySocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final SocketAddress address = new InetSocketAddress("localhost", 7878);
        runNettySocketChannelPerformanceTest(IOUringNettySocketChannelType.INSTANCE, address);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type, final SocketAddress address)
            throws InterruptedException {
        final NettySocketChannel serverChannel = new NettySocketChannel(type, address, true, MESSAGE_SIZE);
        final NettySocketChannel clientChannel = new NettySocketChannel(type, address, false, MESSAGE_SIZE);
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettySocketSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBuffer> requestReader = new NettySocketSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettySocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettySocketSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBuffer> responseReader = new NettySocketSynchronousReader(clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
