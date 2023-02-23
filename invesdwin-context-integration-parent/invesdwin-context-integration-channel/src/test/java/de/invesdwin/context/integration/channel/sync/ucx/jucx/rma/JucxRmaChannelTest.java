package de.invesdwin.context.integration.channel.sync.ucx.jucx.rma;

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
public class JucxRmaChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNioJucxPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNioJucxPerformanceTest(final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testJucxPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newJucxSynchronousReader(
            final JucxRmaSynchronousChannel channel) {
        return new JucxRmaSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newJucxSynchronousWriter(
            final JucxRmaSynchronousChannel channel) {
        return new JucxRmaSynchronousWriter(channel);
    }

    protected JucxRmaSynchronousChannel newJucxSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new JucxRmaSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
