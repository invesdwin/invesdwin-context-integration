package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class BidiChronicleNetworkChannelTest extends AChannelTest {

    @Test
    public void testChronicleSocketPerformance() throws InterruptedException {
        final int port = NetworkUtil.findAvailableTcpPort();
        final InetSocketAddress address = new InetSocketAddress("localhost", port);
        runChronicleSocketPerformanceTest(ChronicleSocketChannelType.VANILLA, address);
    }

    private void runChronicleSocketPerformanceTest(final ChronicleSocketChannelType type,
            final InetSocketAddress address) throws InterruptedException {
        final ChronicleNetworkSynchronousChannel serverChannel = newChronicleNetworkSynchronousChannel(type, address,
                true, getMaxMessageSize());
        final ChronicleNetworkSynchronousChannel clientChannel = newChronicleNetworkSynchronousChannel(type, address,
                false, getMaxMessageSize());

        final ISynchronousWriter<IByteBufferWriter> responseWriter = new ChronicleNetworkSynchronousWriter(
                serverChannel);
        final ISynchronousReader<IByteBuffer> requestReader = new ChronicleNetworkSynchronousReader(serverChannel);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runChronicleSocketPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new ChronicleNetworkSynchronousWriter(
                clientChannel);
        final ISynchronousReader<IByteBuffer> responseReader = new ChronicleNetworkSynchronousReader(clientChannel);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    private ChronicleNetworkSynchronousChannel newChronicleNetworkSynchronousChannel(
            final ChronicleSocketChannelType type, final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        return new ChronicleNetworkSynchronousChannel(type, socketAddress, server, estimatedMaxMessageSize);
    }

}