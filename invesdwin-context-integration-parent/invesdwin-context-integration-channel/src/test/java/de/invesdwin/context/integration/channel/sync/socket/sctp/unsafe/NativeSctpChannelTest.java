package de.invesdwin.context.integration.channel.sync.socket.sctp.unsafe;

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
import de.invesdwin.context.integration.channel.sync.socket.sctp.SctpSynchronousChannel;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class NativeSctpChannelTest extends AChannelTest {

    @Test
    public void testNativeSocketPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNativeSocketPerformanceTest(responseAddress, requestAddress);
    }

    protected void runNativeSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new NativeSctpSynchronousWriter(
                newSctpSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new NativeSctpSynchronousReader(
                newSctpSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new NativeSctpSynchronousWriter(
                newSctpSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new NativeSctpSynchronousReader(
                newSctpSynchronousChannel(responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected SctpSynchronousChannel newSctpSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        return new SctpSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
