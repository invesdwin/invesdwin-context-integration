package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class SocketChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final String addr = newAddress();
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress(addr, ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress(addr, ports[1]);
        runNioSocketPerformanceTest(responseAddress, requestAddress);
    }

    protected String newAddress() {
        return "localhost";
    }

    protected void runNioSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newSocketSynchronousWriter(
                newSocketSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newSocketSynchronousReader(
                newSocketSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newSocketSynchronousWriter(
                newSocketSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newSocketSynchronousReader(
                newSocketSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousReader<IByteBufferProvider> newSocketSynchronousReader(
            final SocketSynchronousChannel channel) {
        return new SocketSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newSocketSynchronousWriter(
            final SocketSynchronousChannel channel) {
        return new SocketSynchronousWriter(channel);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
