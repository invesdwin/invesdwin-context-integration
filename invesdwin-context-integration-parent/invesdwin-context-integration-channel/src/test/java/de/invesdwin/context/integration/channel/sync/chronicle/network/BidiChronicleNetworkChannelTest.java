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
public class BidiChronicleNetworkChannelTest extends AChannelTest {

    @Test
    public void testChronicleSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runChronicleSocketPerformanceTest(ChronicleSocketChannelType.UNSAFE_FAST, address);
    }

    private void runChronicleSocketPerformanceTest(final ChronicleSocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final ChronicleNetworkSynchronousChannel serverChannel = newChronicleNetworkSynchronousChannel(type, address,
                true, getMaxMessageSize());
        final ChronicleNetworkSynchronousChannel clientChannel = newChronicleNetworkSynchronousChannel(type, address,
                false, getMaxMessageSize());
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new ChronicleNetworkSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new ChronicleNetworkSynchronousReader(
                serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new ChronicleNetworkSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new ChronicleNetworkSynchronousReader(
                clientChannel);
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
