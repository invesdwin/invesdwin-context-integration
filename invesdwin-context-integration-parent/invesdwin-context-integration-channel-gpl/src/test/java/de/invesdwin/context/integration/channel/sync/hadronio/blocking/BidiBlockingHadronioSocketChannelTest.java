package de.invesdwin.context.integration.channel.sync.hadronio.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BidiBlockingSocketChannelTest;

@NotThreadSafe
public class BidiBlockingHadronioSocketChannelTest extends BidiBlockingSocketChannelTest {

    @Override
    protected BlockingHadronioSocketSynchronousChannel newBlockingSocketSynchronousChannel(
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingHadronioSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }
}
