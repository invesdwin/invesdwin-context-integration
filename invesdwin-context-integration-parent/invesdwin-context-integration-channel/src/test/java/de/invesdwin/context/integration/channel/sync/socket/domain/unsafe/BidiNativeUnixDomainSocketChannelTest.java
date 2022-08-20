package de.invesdwin.context.integration.channel.sync.socket.domain.unsafe;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;

import de.invesdwin.context.integration.channel.sync.socket.tcp.unsafe.BidiNativeSocketChannelTest;

@Disabled
@NotThreadSafe
public class BidiNativeUnixDomainSocketChannelTest extends BidiNativeSocketChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16, thus commented out)
    //    @Override
    //    @Test
    //    public void testBidiNioSocketPerformance() throws InterruptedException {
    //        final SocketAddress address = java.net.UnixDomainSocketAddress
    //                .of(newFile("testBidiNioSocketPerformance", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        runNioSocketPerformanceTest(address);
    //    }
    //
    //    @Override
    //    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
    //            final boolean server, final int estimatedMaxMessageSize) {
    //        final StandardProtocolFamily protocolFamily = StandardProtocolFamily.UNIX;
    //        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize) {
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
    //    }

}
