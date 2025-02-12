package de.invesdwin.context.integration.channel.sync.mina;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.transport.vmpipe.VmPipeAddress;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mina.type.IMinaSocketType;
import de.invesdwin.context.integration.channel.sync.mina.type.MinaSocketType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class MinaSocketChannelTest extends ALatencyChannelTest {

    @Test
    public void testMinaSocketChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runMinaSocketChannelPerformanceTest(MinaSocketType.NioTcp, responseAddress, requestAddress);
    }

    @Disabled("does not work at all")
    @Test
    public void testMinaDatagramChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableUdpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runMinaSocketChannelPerformanceTest(MinaSocketType.NioUdp, responseAddress, requestAddress);
    }

    @Test
    public void testMinaVmPipeChannelPerformance() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final VmPipeAddress responseAddress = new VmPipeAddress(ports[0]);
        final VmPipeAddress requestAddress = new VmPipeAddress(ports[1]);
        runMinaSocketChannelPerformanceTest(MinaSocketType.VmPipe, responseAddress, requestAddress);
    }

    private void runMinaSocketChannelPerformanceTest(final IMinaSocketType type, final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new MinaSocketSynchronousWriter(
                newMinaSocketChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = new MinaSocketSynchronousReader(
                newMinaSocketChannel(type, requestAddress, false, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new MinaSocketSynchronousWriter(
                newMinaSocketChannel(type, requestAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = new MinaSocketSynchronousReader(
                newMinaSocketChannel(type, responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        runLatencyTest(serverTask, clientTask);
    }

    protected MinaSocketSynchronousChannel newMinaSocketChannel(final IMinaSocketType type,
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new MinaSocketSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
