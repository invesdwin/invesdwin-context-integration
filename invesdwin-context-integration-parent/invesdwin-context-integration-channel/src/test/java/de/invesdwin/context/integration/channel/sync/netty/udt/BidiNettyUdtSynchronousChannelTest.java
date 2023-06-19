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
public class BidiNettyUdtSynchronousChannelTest extends AChannelTest {

    @Test
    public void testBidiNettyUdtChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiNettyUdtChannelPerformanceTest(NioNettyUdtChannelType.INSTANCE, address);
    }

    private void runBidiNettyUdtChannelPerformanceTest(final INettyUdtChannelType type, final InetSocketAddress address)
            throws InterruptedException {
        final NettyUdtSynchronousChannel serverChannel = newNettyUdtChannel(type, address, true, getMaxMessageSize());
        final NettyUdtSynchronousChannel clientChannel = newNettyUdtChannel(type, address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyUdtSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettyUdtSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNettyUdtChannelPerformanceTest", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyUdtSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyUdtSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected NettyUdtSynchronousChannel newNettyUdtChannel(final INettyUdtChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettyUdtSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
