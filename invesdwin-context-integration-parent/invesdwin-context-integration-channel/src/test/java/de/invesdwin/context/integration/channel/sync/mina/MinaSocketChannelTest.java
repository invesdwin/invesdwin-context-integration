package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class MinaSocketChannelTest extends AChannelTest {

    @Test
    public void testMinaSocketChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runMinaSocketChannelPerformanceTest(MinaSocketType.NioTcp, responseAddress, requestAddress);
    }

    private void runMinaSocketChannelPerformanceTest(final IMinaSocketType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaSocketSynchronousWriter(
                newMinaSocketChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaSocketSynchronousReader(
                newMinaSocketChannel(type, requestAddress, false, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runMinaSocketChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaSocketSynchronousWriter(
                newMinaSocketChannel(type, requestAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaSocketSynchronousReader(
                newMinaSocketChannel(type, responseAddress, false, getMaxMessageSize()));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new MinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
