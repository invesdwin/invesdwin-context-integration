package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

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
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNativeSocketLatency() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeSocketLatencyTest(responseAddress, requestAddress);
    }

    protected void runNativeSocketLatencyTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSocketSynchronousWriter(
                newSocketSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSocketSynchronousReader(
                newSocketSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeSocketSynchronousWriter(
                newSocketSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeSocketSynchronousReader(
                newSocketSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    @Test
    public void testNativeSocketThroughput() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress channelAddress = new InetSocketAddress("localhost", port);
        runNativeSocketThroughputTest(channelAddress);
    }

    protected void runNativeSocketThroughputTest(final SocketAddress channelAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> channelWriter = new NativeSocketSynchronousWriter(
                newSocketSynchronousChannel(channelAddress, true, getMaxMessageSize()));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(newSerdeWriter(channelWriter));
        final ISynchronousReader<IByteBufferProvider> channelReader = new NativeSocketSynchronousReader(
                newSocketSynchronousChannel(channelAddress, false, getMaxMessageSize()));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, newSerdeReader(channelReader));
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize) {
            @Override
            protected int newSocketSize(final int estimatedMaxMessageSize) {
                return super.newSocketSize(estimatedMaxMessageSize);
            }
        };
    }

}
