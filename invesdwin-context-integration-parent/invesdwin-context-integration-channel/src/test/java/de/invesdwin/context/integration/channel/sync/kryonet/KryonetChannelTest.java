package de.invesdwin.context.integration.channel.sync.kryonet;

import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.lang.uri.Addresses;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

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
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new KryonetSynchronousWriter(address,
                responseTcpPort, responseUdpPort, true);
        final ISynchronousReader<IByteBuffer> requestReader = new KryonetSynchronousReader(address, requestTcpPort,
                requestUdpPort, false);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKryonetPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new KryonetSynchronousWriter(address,
                requestTcpPort, requestUdpPort, true);
        final ISynchronousReader<IByteBuffer> responseReader = new KryonetSynchronousReader(address, responseTcpPort,
                responseUdpPort, false);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
