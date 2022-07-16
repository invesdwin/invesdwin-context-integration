package de.invesdwin.context.integration.channel.sync.socket.tcp;

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
public class SocketChannelTest extends AChannelTest {

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNioSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
                getMaxMessageSize());
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                getMaxMessageSize());
        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
                getMaxMessageSize());
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
