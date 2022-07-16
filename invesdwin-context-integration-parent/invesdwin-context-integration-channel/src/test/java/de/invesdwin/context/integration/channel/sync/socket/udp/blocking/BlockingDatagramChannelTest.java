package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class BlockingDatagramChannelTest extends AChannelTest {
    @Test
    public void testBlockingDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 37878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 37879);
        runBlockingDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingDatagramSynchronousWriter(
                responseAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingDatagramSynchronousReader(requestAddress,
                getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingDatagramSynchronousWriter(
                requestAddress, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingDatagramSynchronousReader(responseAddress,
                getMaxMessageSize());
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
