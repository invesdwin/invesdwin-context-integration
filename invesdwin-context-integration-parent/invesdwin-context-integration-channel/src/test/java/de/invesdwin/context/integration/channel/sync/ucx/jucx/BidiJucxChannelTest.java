package de.invesdwin.context.integration.channel.sync.ucx.jucx;

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
public class BidiJucxChannelTest extends AChannelTest {

    @Test
    public void testBidiJucxPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runJucxPerformanceTest(address);
    }

    protected void runJucxPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final JucxSynchronousChannel serverChannel = newJucxSynchronousChannel(address, true, getMaxMessageSize());
        final JucxSynchronousChannel clientChannel = newJucxSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiJucxPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newJucxSynchronousReader(final JucxSynchronousChannel channel) {
        return new JucxSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newJucxSynchronousWriter(final JucxSynchronousChannel channel) {
        return new JucxSynchronousWriter(channel);
    }

    protected JucxSynchronousChannel newJucxSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new JucxSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
