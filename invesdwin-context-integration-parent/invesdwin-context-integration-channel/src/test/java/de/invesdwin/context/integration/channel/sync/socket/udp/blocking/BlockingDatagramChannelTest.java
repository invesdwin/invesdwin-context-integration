package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@Disabled("hangs sometimes in testsuite")
@NotThreadSafe
public class BlockingDatagramChannelTest extends AChannelTest {
    @Test
    public void testBlockingDatagramSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runBlockingDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new BlockingDatagramSynchronousWriter(
                newDatagramSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new BlockingDatagramSynchronousReader(
                newDatagramSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingDatagramSynchronousWriter(
                newDatagramSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingDatagramSynchronousReader(
                newDatagramSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected BlockingDatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress responseAddress,
            final boolean server, final int maxMessageSize) {
        return new BlockingDatagramSynchronousChannel(responseAddress, server, maxMessageSize);
    }

}
