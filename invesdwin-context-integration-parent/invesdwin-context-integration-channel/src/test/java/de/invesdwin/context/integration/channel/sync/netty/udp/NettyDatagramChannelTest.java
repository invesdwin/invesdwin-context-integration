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
import de.invesdwin.context.integration.channel.sync.netty.udp.type.NioNettyDatagramChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NettyDatagramChannelTest extends AChannelTest {

    @Test
    public void testNettyDatagramChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettyDatagramChannelPerformanceTest(NioNettyDatagramChannelType.INSTANCE, responseAddress, requestAddress);
    }

    private void runNettyDatagramChannelPerformanceTest(final INettyDatagramChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyDatagramSynchronousWriter(
                newNettyDatagramChannel(type, responseAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettyDatagramSynchronousReader(
                newNettyDatagramChannel(type, requestAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyDatagramSynchronousWriter(
                newNettyDatagramChannel(type, requestAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyDatagramSynchronousReader(
                newNettyDatagramChannel(type, responseAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected NettyDatagramSynchronousChannel newNettyDatagramChannel(final INettyDatagramChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new NettyDatagramSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

}
