package de.invesdwin.context.integration.channel.sync.jucx.stream;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.stream.JucxStreamSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.jucx.stream.JucxStreamSynchronousReader;
import de.invesdwin.context.integration.channel.sync.jucx.stream.JucxStreamSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiJucxStreamChannelTest extends AChannelTest {

    @Test
    public void testBidiJucxPerfoStreamnce() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runJucxPerfoStreamnceTest(address);
    }

    protected void runJucxPerfoStreamnceTest(final InetSocketAddress address) throws InterruptedException {
        final JucxStreamSynchronousChannel serverChannel = newJucxSynchronousChannel(address, true,
                getMaxMessageSize());
        final JucxStreamSynchronousChannel clientChannel = newJucxSynchronousChannel(address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiJucxPerfoStreamnce", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newJucxSynchronousReader(
            final JucxStreamSynchronousChannel channel) {
        return new JucxStreamSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newJucxSynchronousWriter(
            final JucxStreamSynchronousChannel channel) {
        return new JucxStreamSynchronousWriter(channel);
    }

    protected JucxStreamSynchronousChannel newJucxSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new JucxStreamSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
