package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class SocketChannelTest extends AChannelTest {

    protected String newAddress() {
        return "localhost";
    }

    @Test
    public void testNioSocketLatency() throws InterruptedException {
        final String addr = newAddress();
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress(addr, ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress(addr, ports[1]);
        runNioSocketLatencyTest(responseAddress, requestAddress);
    }

    protected void runNioSocketLatencyTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newSocketSynchronousWriter(
                newSocketSynchronousChannel(responseAddress, true, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = newSocketSynchronousReader(
                newSocketSynchronousChannel(requestAddress, true, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newSocketSynchronousWriter(
                newSocketSynchronousChannel(requestAddress, false, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = newSocketSynchronousReader(
                newSocketSynchronousChannel(responseAddress, false, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    @Test
    public void testNioSocketThroughput() throws InterruptedException {
        final String addr = newAddress();
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress channelAddress = new InetSocketAddress(addr, port);
        runNioSocketThroughputTest(channelAddress);
    }

    protected void runNioSocketThroughputTest(final SocketAddress channelAddress) throws InterruptedException {
        final boolean lowLatency = false;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newSocketSynchronousWriter(
                newSocketSynchronousChannel(channelAddress, true, getMaxMessageSize(), lowLatency));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(responseWriter));
        final ISynchronousReader<IByteBufferProvider> responseReader = newSocketSynchronousReader(
                newSocketSynchronousChannel(channelAddress, false, getMaxMessageSize(), lowLatency));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, newSerdeReader(responseReader));
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
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
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency) {
            @Override
            protected int newSocketSize(final int estimatedMaxMessageSize) {
                return super.newSocketSize(estimatedMaxMessageSize);
            }
        };
    }

}
