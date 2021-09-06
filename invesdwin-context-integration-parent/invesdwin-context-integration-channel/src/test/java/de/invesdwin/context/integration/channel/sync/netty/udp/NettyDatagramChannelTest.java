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
        final InetSocketAddress address = new InetSocketAddress("localhost", 7878);
        runNettyDatagramChannelPerformanceTest(NioNettyDatagramChannelType.INSTANCE, address);
    }

    private void runNettyDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettyDatagramChannel serverChannel = new NettyDatagramChannel(type, address, true, MESSAGE_SIZE);
        final NettyDatagramChannel clientChannel = new NettyDatagramChannel(type, address, false, MESSAGE_SIZE);
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NettyDatagramSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBuffer> requestReader = new NettyDatagramSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettyDatagramChannelPerformanceTest",
                1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NettyDatagramSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBuffer> responseReader = new NettyDatagramSynchronousReader(clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
