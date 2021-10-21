package de.invesdwin.context.integration.channel.sync.socket.udp.unsafe;

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
public class NativeDatagramChannelTest extends AChannelTest {
    @Test
    public void testNativeDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 34878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 34879);
        runNativeDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNativeDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new NativeDatagramSynchronousWriter(
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new NativeDatagramSynchronousReader(requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NativeDatagramSynchronousWriter(requestAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new NativeDatagramSynchronousReader(responseAddress,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
