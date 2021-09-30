package de.invesdwin.context.integration.channel.sync.socket.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketSynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class SocketChannelTest extends AChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16, thus commented out)
    //    @Test
    //    public void testUnixDomainSocketSocketPerformance() throws InterruptedException {
    //        final SocketAddress responseAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("response", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        final SocketAddress requestAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("request", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        runUnixDomainSocketPerformanceTest(responseAddress, requestAddress, StandardProtocolFamily.UNIX);
    //    }
    //
    //    private void runUnixDomainSocketPerformanceTest(final SocketAddress responseAddress,
    //            final SocketAddress requestAddress, final ProtocolFamily protocolFamily) throws InterruptedException {
    //        final ISynchronousWriter<IByteBufferWriter> responseWriter = new SocketSynchronousWriter(responseAddress, true,
    //                MESSAGE_SIZE) {
    //            @Override
    //            protected ServerSocketChannel newServerSocketChannel() throws IOException {
    //                return ServerSocketChannel.open(protocolFamily);
    //            }
    //
    //            @Override
    //            protected SocketChannel newSocketChannel() throws IOException {
    //                return SocketChannel.open(protocolFamily);
    //            }
    //        };
    //        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
    //                MESSAGE_SIZE) {
    //            @Override
    //            protected ServerSocketChannel newServerSocketChannel() throws IOException {
    //                return ServerSocketChannel.open(protocolFamily);
    //            }
    //
    //            @Override
    //            protected SocketChannel newSocketChannel() throws IOException {
    //                return SocketChannel.open(protocolFamily);
    //            }
    //        };
    //        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
    //        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
    //        final ISynchronousWriter<IByteBufferWriter> requestWriter = new SocketSynchronousWriter(requestAddress, false,
    //                MESSAGE_SIZE) {
    //            @Override
    //            protected ServerSocketChannel newServerSocketChannel() throws IOException {
    //                return ServerSocketChannel.open(protocolFamily);
    //            }
    //
    //            @Override
    //            protected SocketChannel newSocketChannel() throws IOException {
    //                return SocketChannel.open(protocolFamily);
    //            }
    //        };
    //        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
    //                MESSAGE_SIZE) {
    //            @Override
    //            protected ServerSocketChannel newServerSocketChannel() throws IOException {
    //                return ServerSocketChannel.open(protocolFamily);
    //            }
    //
    //            @Override
    //            protected SocketChannel newSocketChannel() throws IOException {
    //                return SocketChannel.open(protocolFamily);
    //            }
    //        };
    //        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
    //        executor.shutdown();
    //        executor.awaitTermination();
    //    }

    @Test
    public void testNioSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNioSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioSocketPerformanceTest(final SocketAddress responseAddress, final SocketAddress requestAddress)
            throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
                MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

    @Test
    public void testBlockingSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runBlockingSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingSocketSynchronousWriter(
                responseAddress, true, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingSocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingSocketSynchronousWriter(requestAddress,
                false, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingSocketSynchronousReader(responseAddress,
                false, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
