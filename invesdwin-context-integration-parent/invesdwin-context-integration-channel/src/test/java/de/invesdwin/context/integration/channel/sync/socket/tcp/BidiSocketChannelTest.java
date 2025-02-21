package de.invesdwin.context.integration.channel.sync.socket.tcp;

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
public class BidiSocketChannelTest extends AChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final String addr = newAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runNioSocketPerformanceTest(address);
    }

    protected String newAddress() {
        return "localhost";
    }

    protected void runNioSocketPerformanceTest(final SocketAddress address) throws InterruptedException {
        final boolean lowLatency = true;
        final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(address, true, getMaxMessageSize(),
                lowLatency);
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false, getMaxMessageSize(),
                lowLatency);

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newSocketSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newSocketSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newSocketSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newSocketSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousReader<IByteBufferProvider> newSocketSynchronousReader(
            final SocketSynchronousChannel channel) {
        return new SocketSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newSocketSynchronousWriter(
            final SocketSynchronousChannel channel) {
        return new SocketSynchronousWriter(channel);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

}
