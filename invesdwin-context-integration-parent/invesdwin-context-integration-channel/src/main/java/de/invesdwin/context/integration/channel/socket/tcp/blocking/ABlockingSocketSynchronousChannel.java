package de.invesdwin.context.integration.channel.socket.tcp.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.socket.udp.blocking.ABlockingDatagramSocketSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ABlockingSocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected Socket socket;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected ServerSocket serverSocket;

    public ABlockingSocketSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocket = new ServerSocket();
            serverSocket.bind(socketAddress);
            socket = serverSocket.accept();
        } else {
            for (int tries = 0;; tries++) {
                try {
                    socket = new Socket();
                    socket.connect(socketAddress);
                    break;
                } catch (final ConnectException e) {
                    socket.close();
                    socket = null;
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
        socket.setTrafficClass(ABlockingDatagramSocketSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        socket.setReceiveBufferSize(socketSize);
        socket.setSendBufferSize(socketSize);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    protected Duration getConnectRetryDelay() {
        return Duration.ONE_SECOND;
    }

    protected int getMaxConnectRetries() {
        return 10;
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
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
