package de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NativeSocketChannelTest extends AChannelTest {

    @Test
    public void testNativeSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNativeSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNativeSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NativeSocketSynchronousWriter(responseAddress,
                true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new NativeSocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNativeSocketPerformanceTest", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NativeSocketSynchronousWriter(requestAddress,
                false, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new NativeSocketSynchronousReader(responseAddress, false,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
