package de.invesdwin.context.integration.channel.chronicle.network;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class ChronicleNetworkChannelTest extends AChannelTest {

    @Test
    public void testChronicleSocketPerformance() throws InterruptedException {
        final InetSocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final InetSocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runChronicleSocketPerformanceTest(ChronicleSocketChannelType.UNSAFE_FAST_JAVA_8, responseAddress,
                requestAddress);
    }

    private void runChronicleSocketPerformanceTest(final ChronicleSocketChannelType type,
            final InetSocketAddress responseAddress, final InetSocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new ChronicleNetworkSynchronousWriter(type,
                responseAddress, true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new ChronicleNetworkSynchronousReader(type,
                requestAddress, true, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runChronicleSocketPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new ChronicleNetworkSynchronousWriter(type,
                requestAddress, false, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new ChronicleNetworkSynchronousReader(type,
                responseAddress, false, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
