package de.invesdwin.context.integration.channel.sync.disni.active;

import java.net.InetSocketAddress;

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
public class BidiDisniActiveChannelTest extends AChannelTest {

    @Test
    public void testBidiDisniPerformance() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runDisniPerformanceTest(address);
    }

    protected void runDisniPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final DisniActiveSynchronousChannel serverChannel = newDisniSynchronousChannel(address, true,
                getMaxMessageSize());
        final DisniActiveSynchronousChannel clientChannel = newDisniSynchronousChannel(address, false,
                getMaxMessageSize());
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newDisniSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newDisniSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newDisniSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newDisniSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousReader<IByteBufferProvider> newDisniSynchronousReader(
            final DisniActiveSynchronousChannel channel) {
        return new DisniActiveSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newDisniSynchronousWriter(
            final DisniActiveSynchronousChannel channel) {
        return new DisniActiveSynchronousWriter(channel);
    }

    protected DisniActiveSynchronousChannel newDisniSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DisniActiveSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
