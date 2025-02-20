package de.invesdwin.context.integration.channel.sync.netty.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.udp.type.INettyDatagramChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiNettyDatagramChannelTest extends AChannelTest {

    @Test
    public void testBidiNettyDatagramChannelPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runBidiNettyDatagramChannelPerformanceTest(INettyDatagramChannelType.getDefault(), address);
    }

    private void runBidiNettyDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final NettyDatagramSynchronousChannel serverChannel = newNettyDatagramChannel(type, address, true,
                getMaxMessageSize());
        final NettyDatagramSynchronousChannel clientChannel = newNettyDatagramChannel(type, address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyDatagramSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettyDatagramSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyDatagramSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyDatagramSynchronousReader(
                clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected NettyDatagramSynchronousChannel newNettyDatagramChannel(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new NettyDatagramSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
