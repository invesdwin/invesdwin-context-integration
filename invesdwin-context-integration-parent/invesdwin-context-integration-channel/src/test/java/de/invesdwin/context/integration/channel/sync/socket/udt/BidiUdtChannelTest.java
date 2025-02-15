package de.invesdwin.context.integration.channel.sync.socket.udt;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiUdtChannelTest extends ALatencyChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNioSocketPerformanceTest(address);
    }

    protected void runNioSocketPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final UdtSynchronousChannel serverChannel = newUdtSynchronousChannel(address, true, getMaxMessageSize());
        final UdtSynchronousChannel clientChannel = newUdtSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new UdtSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new UdtSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new UdtSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new UdtSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        runLatencyTest(serverTask, clientTask);
    }

    protected UdtSynchronousChannel newUdtSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new UdtSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
