package de.invesdwin.context.integration.channel.sync.jnetrobust;

import java.net.InetSocketAddress;

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
public class BidiJnetrobustChannelTest extends AChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int[] port = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress addressA = new InetSocketAddress("localhost", port[0]);
        final InetSocketAddress addressB = new InetSocketAddress("localhost", port[1]);
        runNioSocketPerformanceTest(addressA, addressB);
    }

    protected void runNioSocketPerformanceTest(final InetSocketAddress addressA, final InetSocketAddress addressB)
            throws InterruptedException {
        final JnetrobustSynchronousChannel serverChannel = newJnetrobustSynchronousChannel(addressA, addressB);
        final JnetrobustSynchronousChannel clientChannel = newJnetrobustSynchronousChannel(addressB, addressA);
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new JnetrobustSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new JnetrobustSynchronousReader(serverChannel);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new JnetrobustSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new JnetrobustSynchronousReader(clientChannel);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected JnetrobustSynchronousChannel newJnetrobustSynchronousChannel(final InetSocketAddress ourSocketAddress,
            final InetSocketAddress otherSocketAddress) {
        return new JnetrobustSynchronousChannel(ourSocketAddress, otherSocketAddress);
    }

}
