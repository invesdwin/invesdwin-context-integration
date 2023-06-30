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
public class DatagramChannelTest extends AChannelTest {

    @Test
    public void testNioDatagramSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNioDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new DatagramSynchronousWriter(
                newDatagramSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new DatagramSynchronousReader(
                newDatagramSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new DatagramSynchronousWriter(
                newDatagramSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new DatagramSynchronousReader(
                newDatagramSynchronousChannel(responseAddress, true, getMaxMessageSize()));
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
