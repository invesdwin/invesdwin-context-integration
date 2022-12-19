package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

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
public class NativeDatagramChannelTest extends AChannelTest {
    @Test
    public void testNativeDatagramSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNativeDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeDatagramSynchronousWriter(
                responseAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> requestReader = new NativeDatagramSynchronousReader(
                requestAddress, getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeDatagramSynchronousWriter(
                requestAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeDatagramSynchronousReader(
                responseAddress, getMaxMessageSize());
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Override
    protected int getMaxMessageSize() {
        return 12;
    }

}
