package de.invesdwin.context.integration.channel.sync.hadronio.blocking;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketChannelTest;

@NotThreadSafe
public class BlockingHadronioSocketChannelTest extends BlockingSocketChannelTest {

    @Override
    protected BlockingHadronioSocketSynchronousChannel newBlockingSocketSynchronousChannel(
            final SocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize,
            final boolean lowLatency) {
        return new BlockingHadronioSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    @Override
    protected String newAddress() {
        return findLocalNetworkAddress();
    }

}
