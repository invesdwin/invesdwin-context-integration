package de.invesdwin.context.integration.channel.sync.disni.passive;

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
public class DisniPassiveChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("192.168.0.20", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("192.168.0.20", ports[1]);
        runNioDisniPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNioDisniPerformanceTest(final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newDisniSynchronousWriter(
                newDisniSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newDisniSynchronousReader(
                newDisniSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDisniPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newDisniSynchronousWriter(
                newDisniSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newDisniSynchronousReader(
                newDisniSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newDisniSynchronousReader(
            final DisniPassiveSynchronousChannel channel) {
        return new DisniPassiveSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newDisniSynchronousWriter(
            final DisniPassiveSynchronousChannel channel) {
        return new DisniPassiveSynchronousWriter(channel);
    }

    protected DisniPassiveSynchronousChannel newDisniSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DisniPassiveSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}