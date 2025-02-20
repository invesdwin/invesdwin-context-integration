package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

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
public class BidiBlockingSocketChannelTest extends AChannelTest {

    @Test
    public void testBlockingSocketPerformance() throws InterruptedException {
        final String addr = newAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress(addr, port);
        runBlockingSocketPerformanceTest(address);
    }

    protected String newAddress() {
        return "localhost";
    }

    protected void runBlockingSocketPerformanceTest(final SocketAddress address) throws InterruptedException {
        final BlockingSocketSynchronousChannel serverChannel = newBlockingSocketSynchronousChannel(address, true,
                getMaxMessageSize());
        final BlockingSocketSynchronousChannel clientChannel = newBlockingSocketSynchronousChannel(address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new BlockingSocketSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new BlockingSocketSynchronousReader(
                serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingSocketSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingSocketSynchronousReader(
                clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected BlockingSocketSynchronousChannel newBlockingSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
