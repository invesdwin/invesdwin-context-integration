package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DatagramChannelTest extends AChannelTest {

    @Test
    public void testNioDatagramSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNioDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new DatagramSynchronousWriter(
                newDatagramSynchronousChannel(responseAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = new DatagramSynchronousReader(
                newDatagramSynchronousChannel(requestAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new DatagramSynchronousWriter(
                newDatagramSynchronousChannel(requestAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = new DatagramSynchronousReader(
                newDatagramSynchronousChannel(responseAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
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
