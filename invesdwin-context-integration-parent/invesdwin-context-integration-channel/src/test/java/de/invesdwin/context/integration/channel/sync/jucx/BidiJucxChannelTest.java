package de.invesdwin.context.integration.channel.sync.jucx;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.context.integration.channel.sync.jucx.type.JucxTransportType;
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
        runJucxPerformanceTest(JucxTransportType.DEFAULT, address);
    }

    protected void runJucxPerformanceTest(final IJucxTransportType type, final InetSocketAddress address)
            throws InterruptedException {
        final JucxSynchronousChannel serverChannel = newJucxSynchronousChannel(type, address, true,
                getMaxMessageSize());
        final JucxSynchronousChannel clientChannel = newJucxSynchronousChannel(type, address, false,
                getMaxMessageSize());

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

    protected JucxSynchronousChannel newJucxSynchronousChannel(final IJucxTransportType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new JucxSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}
