package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

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
public class BidiSctpChannelTest extends AChannelTest {

    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runNioSocketPerformanceTest(address);
    }

    protected void runNioSocketPerformanceTest(final SocketAddress address) throws InterruptedException {
        final SctpSynchronousChannel serverChannel = newSctpSynchronousChannel(address, true, getMaxMessageSize());
        final SctpSynchronousChannel clientChannel = newSctpSynchronousChannel(address, false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = new SctpSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = new SctpSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiSocketPerformance", 1);
        executor.execute(new ServerTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = new SctpSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = new SctpSynchronousReader(clientChannel);
        new ClientTask(newCommandWriter(requestWriter), newCommandReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected SctpSynchronousChannel newSctpSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        return new SctpSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
