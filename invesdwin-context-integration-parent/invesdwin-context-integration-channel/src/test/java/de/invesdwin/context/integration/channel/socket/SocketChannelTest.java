package de.invesdwin.context.integration.channel.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.socket.tcp.SocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.tcp.SocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.tcp.blocking.BlockingSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.tcp.blocking.BlockingSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.udp.DatagramSocketSynchronousWriter;
import de.invesdwin.context.integration.channel.socket.udp.blocking.BlockingDatagramSocketSynchronousReader;
import de.invesdwin.context.integration.channel.socket.udp.blocking.BlockingDatagramSocketSynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class SocketChannelTest extends AChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/
    //    @Test
    //    public void testUnixDomainSocketSocketPerformance() throws InterruptedException {
    //        final SocketAddress responseAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("response", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        final SocketAddress requestAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("request", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        runUnixDomainSocketPerformanceTest(responseAddress, requestAddress, StandardProtocolFamily.UNIX);
    //    }

    @SuppressWarnings("unused")
    private void runUnixDomainSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress, final ProtocolFamily protocolFamily) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new SocketSynchronousWriter(responseAddress, true,
                MESSAGE_SIZE) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
        final ISynchronousReader<IByteBuffer> requestReader = new SocketSynchronousReader(requestAddress, true,
                MESSAGE_SIZE) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new SocketSynchronousWriter(requestAddress, false,
                MESSAGE_SIZE) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
        final ISynchronousReader<IByteBuffer> responseReader = new SocketSynchronousReader(responseAddress, false,
                MESSAGE_SIZE) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

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
    public void testNioDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runNioDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runNioDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new DatagramSocketSynchronousWriter(
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new DatagramSocketSynchronousReader(requestAddress,
                MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new DatagramSocketSynchronousWriter(requestAddress,
                MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new DatagramSocketSynchronousReader(responseAddress,
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

    @Test
    public void testBlockingDatagramSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = new InetSocketAddress("localhost", 7878);
        final SocketAddress requestAddress = new InetSocketAddress("localhost", 7879);
        runBlockingDatagramSocketPerformanceTest(responseAddress, requestAddress);
    }

    private void runBlockingDatagramSocketPerformanceTest(final SocketAddress responseAddress,
            final SocketAddress requestAddress) throws InterruptedException {
        final ISynchronousWriter<IByteBufferWriter> responseWriter = new BlockingDatagramSocketSynchronousWriter(
                responseAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> requestReader = new BlockingDatagramSocketSynchronousReader(
                requestAddress, MESSAGE_SIZE);
        final WrappedExecutorService executor = Executors.newFixedThreadPool("testDatagramSocketPerformance", 1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new BlockingDatagramSocketSynchronousWriter(
                requestAddress, MESSAGE_SIZE);
        final ISynchronousReader<IByteBuffer> responseReader = new BlockingDatagramSocketSynchronousReader(
                responseAddress, MESSAGE_SIZE);
        read(newCommandWriter(requestWriter), newCommandReader(responseReader));
        executor.shutdown();
        executor.awaitTermination();
    }

}
