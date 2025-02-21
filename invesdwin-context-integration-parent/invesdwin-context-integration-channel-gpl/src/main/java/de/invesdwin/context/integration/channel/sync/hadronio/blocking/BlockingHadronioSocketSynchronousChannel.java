package de.invesdwin.context.integration.channel.sync.hadronio.blocking;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.hadronio.HadronioSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.tcp.blocking.BlockingSocketSynchronousChannel;

@NotThreadSafe
public class BlockingHadronioSocketSynchronousChannel extends BlockingSocketSynchronousChannel {

    public BlockingHadronioSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize, final boolean lowLatency) {
        super(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    @Override
    protected void configureSocket(final Socket socket) throws SocketException {
        //not supported
    }

    @Override
    protected SocketChannel newSocketChannel() throws IOException {
        return HadronioSocketSynchronousChannel.HADRONIO_PROVIDER.openSocketChannel();
    }

    @Override
    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return HadronioSocketSynchronousChannel.HADRONIO_PROVIDER.openServerSocketChannel();
    }

}
