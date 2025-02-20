package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiNettySocketChannelTest extends AChannelTest {

    @Test
    public void testBidiNettySocketChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiNettySocketChannelPerformanceTest(INettySocketChannelType.getDefault(), address);
    }

    private void runBidiNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettySocketSynchronousChannel serverChannel = newNettySocketChannel(type, address, true,
                getMaxMessageSize());
        final NettySocketSynchronousChannel clientChannel = newNettySocketChannel(type, address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettySocketSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettySocketSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettySocketSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettySocketSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
