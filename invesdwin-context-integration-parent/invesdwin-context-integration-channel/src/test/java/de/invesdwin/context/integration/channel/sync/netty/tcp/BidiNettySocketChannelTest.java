package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.NioNettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class BidiNettySocketChannelTest extends AChannelTest {

    @Test
    public void testBidiNettySocketChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiNettySocketChannelPerformanceTest(NioNettySocketChannelType.INSTANCE, address);
    }

    private void runBidiNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketSynchronousChannel serverChannel = newNettySocketChannel(type, address, true,
                getMaxMessageSize());
        final NettySocketSynchronousChannel clientChannel = newNettySocketChannel(type, address, false,
                getMaxMessageSize());

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

    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
