package de.invesdwin.context.integration.channel.sync.netty.tcp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.NioNettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NettySocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelLatency() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettySocketChannelLatencyTest(NioNettySocketChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettySocketChannelLatencyTest(final INettySocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettySocketSynchronousWriter(
                newNettySocketChannel(type, responseAddress, true, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettySocketSynchronousReader(
                newNettySocketChannel(type, requestAddress, false, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettySocketSynchronousWriter(
                newNettySocketChannel(type, requestAddress, true, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettySocketSynchronousReader(
                newNettySocketChannel(type, responseAddress, false, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    @Test
    public void testNettySocketChannelThroughput() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress channelAddress = new InetSocketAddress("localhost", port);
        runNettySocketChannelThroughputTest(NioNettySocketChannelType.INSTANCE, channelAddress);
    }

    private void runNettySocketChannelThroughputTest(final INettySocketChannelType type,
            final InetSocketAddress channelAddress) throws InterruptedException {
        final boolean lowLatency = false;
        final ISynchronousWriter<IByteBufferProvider> channelWriter = new NettySocketSynchronousWriter(
                newNettySocketChannel(type, channelAddress, true, getMaxMessageSize(), lowLatency));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(channelWriter));
        final ISynchronousReader<IByteBufferProvider> channelReader = new NettySocketSynchronousReader(
                newNettySocketChannel(type, channelAddress, false, getMaxMessageSize(), lowLatency));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, newSerdeReader(channelReader));
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected NettySocketSynchronousChannel newNettySocketChannel(final INettySocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new NettySocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

}
