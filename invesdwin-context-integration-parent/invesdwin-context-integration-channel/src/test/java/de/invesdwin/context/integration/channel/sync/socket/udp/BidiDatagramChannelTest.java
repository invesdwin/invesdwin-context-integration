package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiDatagramChannelTest extends AChannelTest {

    @Test
    public void testDatagramPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableUdpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runDatagramPerformanceTest(address);
    }

    protected void runDatagramPerformanceTest(final SocketAddress address) throws InterruptedException {
        final DatagramSynchronousChannel serverChannel = newDatagramSynchronousChannel(address, true,
                getMaxMessageSize());
        final DatagramSynchronousChannel clientChannel = newDatagramSynchronousChannel(address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new DatagramSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new DatagramSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new DatagramSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new DatagramSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected DatagramSynchronousChannel newDatagramSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DatagramSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    protected int getMaxMessageSize() {
        return 12;
    }

}
