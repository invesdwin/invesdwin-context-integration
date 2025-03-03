package de.invesdwin.context.integration.channel.sync.netty.tcp.unsafe;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.netty.tcp.NettySocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.netty.tcp.type.INettySocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import io.netty.channel.unix.UnixChannel;

@NotThreadSafe
public class NettyNativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNettySocketChannelPerformance() throws InterruptedException {
        final INettySocketChannelType type = INettySocketChannelType.getDefault();
        if (!UnixChannel.class.isAssignableFrom(type.getClientChannelType())) {
            //not supported on windows
            return;
        }
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNettySocketChannelPerformanceTest(type, responseAddress, requestAddress);
    }

    private void runNettySocketChannelPerformanceTest(final INettySocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final boolean lowLatency = true;
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NettyNativeSocketSynchronousWriter(
                new NettySocketSynchronousChannel(type, responseAddress, true, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NettyNativeSocketSynchronousReader(
                new NettySocketSynchronousChannel(type, requestAddress, false, getMaxMessageSize(), lowLatency));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NettyNativeSocketSynchronousWriter(
                new NettySocketSynchronousChannel(type, requestAddress, true, getMaxMessageSize(), lowLatency));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NettyNativeSocketSynchronousReader(
                new NettySocketSynchronousChannel(type, responseAddress, false, getMaxMessageSize(), lowLatency));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

}
