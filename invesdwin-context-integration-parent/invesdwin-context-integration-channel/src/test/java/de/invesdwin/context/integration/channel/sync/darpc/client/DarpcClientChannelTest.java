package de.invesdwin.context.integration.channel.sync.darpc.client;

import java.net.InetSocketAddress;

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
public class DarpcClientChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("192.168.0.20", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("192.168.0.20", ports[1]);
        runNioDarpcPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNioDarpcPerformanceTest(final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newDarpcSynchronousWriter(
                newDarpcSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newDarpcSynchronousReader(
                newDarpcSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDarpcPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newDarpcSynchronousWriter(
                newDarpcSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newDarpcSynchronousReader(
                newDarpcSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newDarpcSynchronousReader(
            final DarpcClientSynchronousChannel channel) {
        return new DarpcClientSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newDarpcSynchronousWriter(
            final DarpcClientSynchronousChannel channel) {
        return new DarpcClientSynchronousWriter(channel);
    }

    protected DarpcClientSynchronousChannel newDarpcSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DarpcClientSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
