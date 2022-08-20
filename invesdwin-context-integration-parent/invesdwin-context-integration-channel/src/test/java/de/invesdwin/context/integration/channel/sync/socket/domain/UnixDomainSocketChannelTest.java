package de.invesdwin.context.integration.channel.sync.socket.domain;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;

import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;

@Disabled
@NotThreadSafe
public class UnixDomainSocketChannelTest extends SocketChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16, thus commented out)
    //    @Override
    //    @Test
    //    public void testNioSocketPerformance() throws InterruptedException {
    //        final SocketAddress responseAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("response", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        final SocketAddress requestAddress = java.net.UnixDomainSocketAddress
    //                .of(newFile("request", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
    //        runNioSocketPerformanceTest(responseAddress, requestAddress);
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
