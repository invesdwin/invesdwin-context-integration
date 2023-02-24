package de.invesdwin.context.integration.channel.sync.jucx.tag;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.jucx.tag.JucxTagSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.jucx.tag.JucxTagSynchronousReader;
import de.invesdwin.context.integration.channel.sync.jucx.tag.JucxTagSynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiJucxTagChannelTest extends AChannelTest {

    @Test
    public void testBidiJucxPerfoTagnce() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runJucxPerfoTagnceTest(address);
    }

    protected void runJucxPerfoTagnceTest(final InetSocketAddress address) throws InterruptedException {
        final JucxTagSynchronousChannel serverChannel = newJucxSynchronousChannel(address, true, getMaxMessageSize());
        final JucxTagSynchronousChannel clientChannel = newJucxSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiJucxPerfoTagnce", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newJucxSynchronousReader(
            final JucxTagSynchronousChannel channel) {
        return new JucxTagSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newJucxSynchronousWriter(
            final JucxTagSynchronousChannel channel) {
        return new JucxTagSynchronousWriter(channel);
    }

    protected JucxTagSynchronousChannel newJucxSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new JucxTagSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
