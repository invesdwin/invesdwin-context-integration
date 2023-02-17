package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNativeSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeSocketPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNativeSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSocketSynchronousWriter(
                newSocketSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSocketSynchronousReader(
                newSocketSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNativeSocketPerformanceTest", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeSocketSynchronousWriter(
                newSocketSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeSocketSynchronousReader(
                newSocketSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
