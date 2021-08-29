package de.invesdwin.context.integration.channel.socket.nio;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.socket.old.udp.AOldDatagramSocketSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ANioSocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected Socket socket;
    protected SocketChannel socketChannel;
    private final SocketAddress socketAddress;
    private final boolean server;
    private ServerSocketChannel serverSocketChannel;
    private ServerSocket serverSocket;

    public ANioSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(socketAddress);
            serverSocket = serverSocketChannel.socket();
            socketChannel = serverSocketChannel.accept();
            socket = socketChannel.socket();
        } else {
            for (int tries = 0;; tries++) {
                socketChannel = SocketChannel.open();
                try {
                    socketChannel.connect(socketAddress);
                    socket = socketChannel.socket();
                    break;
                } catch (final ConnectException e) {
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
        socket.setTrafficClass(
                AOldDatagramSocketSynchronousChannel.IPTOS_LOWDELAY | AOldDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        socket.setReceiveBufferSize(socketSize);
        socket.setSendBufferSize(socketSize);
        socket.setTcpNoDelay(true);
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
        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
