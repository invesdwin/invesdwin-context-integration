package de.invesdwin.context.integration.channel.sync.mina.unsafe.udp;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class MinaNativeDatagramChannelTest extends AChannelTest {

    @Test
    public void testNettyNativeDatagramChannelPerformance() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettyNativeDatagramChannelPerformanceTest(MinaSocketType.AprUdp, responseAddress, requestAddress);
    }

    private void runNettyNativeDatagramChannelPerformanceTest(final IMinaSocketType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaNativeDatagramSynchronousWriter(
                new MinaSocketSynchronousChannel(type, responseAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaNativeDatagramSynchronousReader(
                new MinaSocketSynchronousChannel(type, requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors
                .newFixedThreadPool("runNettyNativeDatagramChannelPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaNativeDatagramSynchronousWriter(
                new MinaSocketSynchronousChannel(type, requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaNativeDatagramSynchronousReader(
                new MinaSocketSynchronousChannel(type, responseAddress, true, getMaxMessageSize()));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
