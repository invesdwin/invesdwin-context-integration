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
public class JucxTagChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerfoTagnce() throws InterruptedException {
        final int[] ports = NetworkUtil.findAvailableTcpPorts(2);
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", ports[0]);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", ports[1]);
        runNioJucxPerfoTagnceTest(responseAddress, requestAddress);
    }

    protected void runNioJucxPerfoTagnceTest(final InetSocketAddress responseAddress,
            final InetSocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferProvider> responseWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(responseAddress, true, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> requestReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(requestAddress, true, getMaxMessageSize()));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testJucxPerfoTagnce", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newJucxSynchronousWriter(
                newJucxSynchronousChannel(requestAddress, false, getMaxMessageSize()));
        final ISynchronousReader<IByteBufferProvider> responseReader = newJucxSynchronousReader(
                newJucxSynchronousChannel(responseAddress, false, getMaxMessageSize()));
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