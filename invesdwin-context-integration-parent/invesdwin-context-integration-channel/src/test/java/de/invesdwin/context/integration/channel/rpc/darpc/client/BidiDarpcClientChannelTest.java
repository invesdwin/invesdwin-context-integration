package de.invesdwin.context.integration.channel.rpc.darpc.client;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class BidiDarpcClientChannelTest extends ALatencyChannelTest {

    @Test
    public void testBidiDarpcPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("192.168.0.20", port);
        runDarpcPerformanceTest(address);
    }

    protected void runDarpcPerformanceTest(final InetSocketAddress address) throws InterruptedException {
        final DarpcClientSynchronousChannel serverChannel = newDarpcSynchronousChannel(address, true,
                getMaxMessageSize());
        final DarpcClientSynchronousChannel clientChannel = newDarpcSynchronousChannel(address, false,
                getMaxMessageSize());

        final ISynchronousWriter<IByteBufferProvider> responseWriter = newDarpcSynchronousWriter(serverChannel);
        final ISynchronousReader<IByteBufferProvider> requestReader = newDarpcSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testBidiDarpcPerformance", 1);
        executor.execute(new LatencyServerTask(newSerdeReader(requestReader), newSerdeWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferProvider> requestWriter = newDarpcSynchronousWriter(clientChannel);
        final ISynchronousReader<IByteBufferProvider> responseReader = newDarpcSynchronousReader(clientChannel);
        new LatencyClientTask(newSerdeWriter(requestWriter), newSerdeReader(responseReader)).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousReader<IByteBufferProvider> newDarpcSynchronousReader(
            final DarpcClientSynchronousChannel channel) {
        return new DarpcClientSynchronousReader(channel);
    }

    protected ISynchronousWriter<IByteBufferProvider> newDarpcSynchronousWriter(
            final DarpcClientSynchronousChannel channel) {
        return new DarpcClientSynchronousWriter(channel);
    }

    protected DarpcClientSynchronousChannel newDarpcSynchronousChannel(final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        return new DarpcClientSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
