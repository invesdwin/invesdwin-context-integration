package de.invesdwin.context.integration.channel.sync.jucx;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.context.integration.channel.sync.jucx.type.JucxTransportType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class JucxChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final String addr = findLocalNetworkAddress();
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress(addr, ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress(addr, ports[1]);
        runNioJucxPerformanceTest(JucxTransportType.DEFAULT, responseAddress, requestAddress);
    }

    protected void runNioJucxPerformanceTest(final IJucxTransportType type, final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(type, responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(type, requestAddress, true, getMaxMessageSize()));
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(type, requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(type, responseAddress, false, getMaxMessageSize()));
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousReader<IByteBufferProvider> newJucxSynchronousReader(final JucxSynchronousChannel channel) {
        return new JucxSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newJucxSynchronousWriter(final JucxSynchronousChannel channel) {
        return new JucxSynchronousWriter(channel);
    }

    protected JucxSynchronousChannel newJucxSynchronousChannel(final IJucxTransportType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new JucxSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
