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
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNativeSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNativeSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NativeSocketSynchronousWriter(
                new SocketSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> requestReader = new NativeSocketSynchronousReader(
                new SocketSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNativeSocketPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NativeSocketSynchronousWriter(
                new SocketSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBuffer> responseReader = new NativeSocketSynchronousReader(
                new SocketSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
