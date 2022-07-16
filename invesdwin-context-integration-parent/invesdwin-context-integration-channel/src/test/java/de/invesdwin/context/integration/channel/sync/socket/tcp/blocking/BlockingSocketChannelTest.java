package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

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
public class BlockingSocketChannelTest extends AChannelTest {

    @Test
    public void testBlockingSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runBlockingSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingSocketSynchronousWriter(
                responseAddress, true, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingSocketSynchronousReader(requestAddress, true,
                getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingSocketSynchronousWriter(requestAddress,
                false, getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingSocketSynchronousReader(responseAddress,
                false, getMaxMessageSize());
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
