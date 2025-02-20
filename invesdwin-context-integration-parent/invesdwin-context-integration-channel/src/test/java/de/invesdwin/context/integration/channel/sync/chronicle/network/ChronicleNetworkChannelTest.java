package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class ChronicleNetworkChannelTest extends AChannelTest {

    @Test
    public void testChronicleSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runChronicleSocketPerformanceTest(ChronicleSocketChannelType.UNSAFE_FAST, responseAddress, requestAddress);
    }

    private void runChronicleSocketPerformanceTest(final ChronicleSocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new ChronicleNetworkSynchronousWriter(
                newChronicleNetworkSynchronousChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new ChronicleNetworkSynchronousReader(
                newChronicleNetworkSynchronousChannel(type, requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new ChronicleNetworkSynchronousWriter(
                newChronicleNetworkSynchronousChannel(type, requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new ChronicleNetworkSynchronousReader(
                newChronicleNetworkSynchronousChannel(type, responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    private ChronicleNetworkSynchronousChannel newChronicleNetworkSynchronousChannel(
            final ChronicleSocketChannelType type, final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        return new ChronicleNetworkSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
