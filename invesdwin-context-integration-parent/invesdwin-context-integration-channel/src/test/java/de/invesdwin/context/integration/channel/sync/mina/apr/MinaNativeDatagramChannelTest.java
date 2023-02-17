package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.OperatingSystem;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class MinaNativeDatagramChannelTest extends AChannelTest {

    @Test
    public void testMinaSocketChannelPerformance() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNativeDatagramSocketPerformanceTest(final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaNativeDatagramSynchronousWriter(
                responseAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaNativeDatagramSynchronousReader(
                requestAddress, getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaNativeDatagramSynchronousWriter(
                requestAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaNativeDatagramSynchronousReader(
                responseAddress, getMaxMessageSize());
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

}
