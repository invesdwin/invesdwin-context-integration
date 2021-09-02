package de.invesdwin.context.integration.channel.socket.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.socket.udp.blocking.ABlockingDatagramSocketSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ASocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected Socket socket;
    protected SocketChannel socketChannel;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected ServerSocketChannel serverSocketChannel;

    public ASocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocketChannel = newServerSocketChannel();
            serverSocketChannel.bind(socketAddress);
            socketChannel = serverSocketChannel.accept();
            try {
                socket = socketChannel.socket();
            } catch (final Throwable t) {
                //unix domain sockets throw an error here
            }
        } else {
            for (int tries = 0;; tries++) {
                socketChannel = newSocketChannel();
                try {
                    socketChannel.connect(socketAddress);
                    try {
                        socket = socketChannel.socket();
                    } catch (final Throwable t) {
                        //unix domain sockets throw an error here
                    }
                    break;
                } catch (final IOException e) {
                    socketChannel.close();
                    socketChannel = null;
                    if (socket != null) {
                        socket.close();
                        socket = null;
                    }
                    if (tries < getMaxConnectRetries()) {
                        try {
                            getConnectRetryDelay().sleep();
                        } catch (final InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
        //non-blocking sockets are a bit faster than blocking ones
        socketChannel.configureBlocking(false);
        if (socket != null) {
            //might be unix domain socket
            socket.setTrafficClass(ABlockingDatagramSocketSynchronousChannel.IPTOS_LOWDELAY
                    | ABlockingDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
            socket.setReceiveBufferSize(socketSize);
            socket.setSendBufferSize(socketSize);
            socket.setTcpNoDelay(true);
        }
    }

    protected SocketChannel newSocketChannel() throws IOException {
        return SocketChannel.open();
    }

    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    @Override
    public void close() throws IOException {
        if (socketChannel != null) {
            socketChannel.close();
            socketChannel = null;
        }
        if (socket != null) {
            socket.close();
            socket = null;
        }
        if (serverSocketChannel != null) {
            serverSocketChannel.close();
            serverSocketChannel = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
