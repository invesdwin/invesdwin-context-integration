package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

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
public class BlockingSocketChannelTest extends AChannelTest {

    @Test
    public void testBlockingSocketPerformance() throws InterruptedException {
        final String addr = newAddress();
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress(addr, ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress(addr, ports[1]);
        runBlockingSocketPerformanceTest(responseAddress, requestAddress);
    }

    protected String newAddress() {
        return "localhost";
    }

    protected void runBlockingSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new BlockingSocketSynchronousWriter(
                newBlockingSocketSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new BlockingSocketSynchronousReader(
                newBlockingSocketSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new BlockingSocketSynchronousWriter(
                newBlockingSocketSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new BlockingSocketSynchronousReader(
                newBlockingSocketSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected BlockingSocketSynchronousChannel newBlockingSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
