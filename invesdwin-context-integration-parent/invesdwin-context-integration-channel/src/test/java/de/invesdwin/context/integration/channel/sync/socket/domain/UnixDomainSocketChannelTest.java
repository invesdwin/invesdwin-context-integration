package de.invesdwin.context.integration.channel.sync.socket.domain;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.lang.OperatingSystem;

@NotThreadSafe
public class UnixDomainSocketChannelTest extends SocketChannelTest {

    //https://nipafx.dev/java-unix-domain-sockets/ (requires java 16)
    @Override
    @Test
    public void testNioSocketLatency() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final SocketAddress responseAddress = java.net.UnixDomainSocketAddress
                .of(newFile("response", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        final SocketAddress requestAddress = java.net.UnixDomainSocketAddress
                .of(newFile("request", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        runNioSocketLatencyTest(responseAddress, requestAddress);
    }

    @Override
    public void testNioSocketThroughput() throws InterruptedException {
        if (OperatingSystem.isWindows()) {
            //not supported on windows
            return;
        }
        final SocketAddress channelAddress = java.net.UnixDomainSocketAddress
                .of(newFile("channel", true, FileChannelType.UNIX_SOCKET).getAbsolutePath());
        runNioSocketThroughputTest(channelAddress);
    }

    @Override
    protected SocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        final StandardProtocolFamily protocolFamily = StandardProtocolFamily.UNIX;
        return new SocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency) {
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
