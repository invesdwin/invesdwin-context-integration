package de.invesdwin.context.integration.channel.sync.disni.active;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiDisniActiveChannelTest extends AChannelTest {

    @Test
    public void testBidiDisniPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("192.168.0.20", port);
        runDisniPerformanceTest(address);
    }

    protected void runDisniPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final DisniActiveSynchronousChannel serverChannel = newDisniSynchronousChannel(address, true,
                getMaxMessageSize());
        final DisniActiveSynchronousChannel clientChannel = newDisniSynchronousChannel(address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newDisniSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newDisniSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiDisniPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newDisniSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newDisniSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newDisniSynchronousReader(
            final DisniActiveSynchronousChannel channel) {
        return new DisniActiveSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newDisniSynchronousWriter(
            final DisniActiveSynchronousChannel channel) {
        return new DisniActiveSynchronousWriter(channel);
    }

    protected DisniActiveSynchronousChannel newDisniSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DisniActiveSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
