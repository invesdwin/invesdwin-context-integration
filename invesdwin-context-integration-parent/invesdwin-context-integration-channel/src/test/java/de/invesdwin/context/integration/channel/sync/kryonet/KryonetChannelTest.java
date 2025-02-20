package de.invesdwin.context.integration.channel.sync.kryonet;

import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.lang.uri.Addresses;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class KryonetChannelTest extends AChannelTest {

    @Test
    public void testKryonetTcpPerformance() throws InterruptedException {
        runKryonetPerformanceTest(Addresses.asAddress("localhost"), 7878, -1, 7879, -1);
    }

    @Test
    public void testKryonetUdpPerformance() throws InterruptedException {
        runKryonetPerformanceTest(Addresses.asAddress("localhost"), 7878, 7878, 8879, 8879);
    }

    private void runKryonetPerformanceTest(final InetAddress address, final int responseTcpPort,
            final int responseUdpPort, final int requestTcpPort, final int requestUdpPort) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = new KryonetSynchronousWriter(address,
                responseTcpPort, responseUdpPort, true);
        final ISynchronousReader<IByteBufferProvider> requestReader = new KryonetSynchronousReader(address,
                requestTcpPort, requestUdpPort, false);
        final LatencyServerTask serverTask = new LatencyServerTask(this, newSerdeReader(requestReader),
                newSerdeWriter(responseWriter));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new KryonetSynchronousWriter(address,
                requestTcpPort, requestUdpPort, true);
        final ISynchronousReader<IByteBufferProvider> responseReader = new KryonetSynchronousReader(address,
                responseTcpPort, responseUdpPort, false);
        final LatencyClientTask clientTask = new LatencyClientTask(this, newSerdeWriter(requestWriter),
                newSerdeReader(responseReader));
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

}
