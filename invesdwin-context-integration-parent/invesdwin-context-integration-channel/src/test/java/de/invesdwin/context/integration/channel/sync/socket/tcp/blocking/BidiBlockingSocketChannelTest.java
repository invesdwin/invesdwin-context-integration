package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiBlockingSocketChannelTest extends ALatencyChannelTest {

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
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new LatencyServerTask(newSerdeReader(requestReader), newSerdeWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingSocketSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingSocketSynchronousReader(
                clientChannel);
        new LatencyClientTask(newSerdeWriter(requestWriter), newSerdeReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected BlockingSocketSynchronousChannel newBlockingSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
