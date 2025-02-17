package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiSocketChannelTest extends ALatencyChannelTest {

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
        final SocketSynchronousChannel serverChannel = newSocketSynchronousChannel(address, true, getMaxMessageSize());
        final SocketSynchronousChannel clientChannel = newSocketSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newSocketSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newSocketSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newSocketSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newSocketSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        runLatencyTest(serverTask, clientTask);
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
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
