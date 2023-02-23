package de.invesdwin.context.integration.channel.sync.ucx.hadronio.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketChannelTest;

@NotThreadSafe
public class BlockingHadronioSocketChannelTest extends BlockingSocketChannelTest {

    @Override
    protected BlockingHadronioSocketSynchronousChannel newBlockingSocketSynchronousChannel(
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        return new BlockingHadronioSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize);
    }

}
