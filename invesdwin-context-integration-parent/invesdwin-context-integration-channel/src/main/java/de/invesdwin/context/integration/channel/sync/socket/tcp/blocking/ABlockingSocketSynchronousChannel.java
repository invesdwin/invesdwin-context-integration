package de.invesdwin.context.integration.channel.sync.socket.tcp.blocking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.ABlockingDatagramSynchronousChannel;
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
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (true) {
                try {
                    socket = new Socket();
                    socket.connect(socketAddress);
                    break;
                } catch (final ConnectException e) {
                    socket.close();
                    socket = null;
                    if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
        socket.setTrafficClass(ABlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
        socket.setReceiveBufferSize(socketSize);
        socket.setSendBufferSize(socketSize);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    protected Duration getMaxConnectRetryDelay() {
        return SynchronousChannels.DEFAULT_MAX_RECONNECT_DELAY;
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
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

}
