package de.invesdwin.context.integration.channel.sync.socket;

import java.io.IOException;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousReader;
import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.NativeSocketSynchronousWriter;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;

@NotThreadSafe
public class NativeUnixDomainSocketChannelTest extends AChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16, thus commented out)
    @Test
    public void testNativeUnixDomainSocketSocketPerformance() throws InterruptedException {
        final SocketAddress responseAddress = java.net.UnixDomainSocketAddress
                .of(newFile("response", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        final SocketAddress requestAddress = java.net.UnixDomainSocketAddress
                .of(newFile("request", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        runNativeUnixDomainSocketPerformanceTest(responseAddress, requestAddress, StandardProtocolFamily.UNIX);
    }

    private void runNativeUnixDomainSocketPerformanceTest(final SocketAddress responseAddress,
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
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runNativeUnixDomainSocketPerformanceTest",
                1);
        executor.execute(new WriterTask(newCommandReader(requestReader), newCommandWriter(responseWriter)));
        final ISynchronousWriter<IByteBufferWriter> requestWriter = new NativeSocketSynchronousWriter(requestAddress,
                false, MESSAGE_SIZE) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
        final ISynchronousReader<IByteBuffer> responseReader = new NativeSocketSynchronousReader(responseAddress, false,
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

}
