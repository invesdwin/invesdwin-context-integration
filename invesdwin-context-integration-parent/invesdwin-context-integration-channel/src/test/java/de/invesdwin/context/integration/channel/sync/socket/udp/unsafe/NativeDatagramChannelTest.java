package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

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
import de.invesdwin.context.integration.channel.sync.socket.udp.DatagramSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeDatagramChannelTest extends AChannelTest {
    @Test
    public void testNativeDatagramSocketLatency() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeDatagramSocketLatencyTest(responseAddress, requestAddress);
    }

    private void runNativeDatagramSocketLatencyTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeDatagramSynchronousWriter(
                newDatagramSynchronousChannel(responseAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NativeDatagramSynchronousReader(
                newDatagramSynchronousChannel(requestAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeDatagramSynchronousWriter(
                newDatagramSynchronousChannel(requestAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeDatagramSynchronousReader(
                newDatagramSynchronousChannel(responseAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    @Test
    public void testNativeDatagramSocketThroughput() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress channelAddress = new InetSocketAddress("localhost", port);
        runNativeDatagramSocketThroughputTest(channelAddress);
    }

    private void runNativeDatagramSocketThroughputTest(final SocketAddress channelAddress) throws InterruptedException {
        final boolean lowLatency = false;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeDatagramSynchronousWriter(
                newDatagramSynchronousChannel(channelAddress, false, getMaxMessageSize(), lowLatency));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(responseWriter));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeDatagramSynchronousReader(
                newDatagramSynchronousChannel(channelAddress, true, getMaxMessageSize(), lowLatency));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, newSerdeReader(responseReader));
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    @Override
    public int getMaxMessageSize() {
        return 12;
    }

}
