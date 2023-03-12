package de.invesdwin.context.integration.channel.sync.hadronio;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.hhu.bsinfo.hadronio.HadronioProvider;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;

@NotThreadSafe
public class HadronioSocketSynchronousChannel extends SocketSynchronousChannel {

    public static final HadronioProvider HADRONIO_PROVIDER = new HadronioProvider();

    public HadronioSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        super(socketAddress, server, estimatedMaxMessageSize);
    }

    @Override
    protected void configureSocket(final Socket socket) throws SocketException {
        //not supported
    }

    @Override
    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return HADRONIO_PROVIDER.openServerSocketChannel();
    }

    @Override
    protected SocketChannel newSocketChannel() throws IOException {
        return HADRONIO_PROVIDER.openSocketChannel();
    }

}
