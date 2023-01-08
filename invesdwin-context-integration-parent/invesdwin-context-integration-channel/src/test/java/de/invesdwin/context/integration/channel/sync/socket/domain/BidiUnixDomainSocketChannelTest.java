package de.invesdwin.context.integration.channel.sync.socket.domain;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.sync.socket.tcp.BidiSocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.lang.OperatingSystem;

@Disabled
@NotThreadSafe
public class BidiUnixDomainSocketChannelTest extends BidiSocketChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16)
    @Override
    @Test
    public void testBidiNioSocketPerformance() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final SocketAddress address = java.net.UnixDomainSocketAddress
                .of(newFile("testBidiNioSocketPerformance", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        runNioSocketPerformanceTest(address);
    }

    @Override
    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        final StandardProtocolFamily protocolFamily = StandardProtocolFamily.UNIX;
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize) {
            @Override
            protected ServerSocketChannel newServerSocketChannel() throws IOException {
                return ServerSocketChannel.open(protocolFamily);
            }

            @Override
            protected SocketChannel newSocketChannel() throws IOException {
                return SocketChannel.open(protocolFamily);
            }
        };
    }

}
