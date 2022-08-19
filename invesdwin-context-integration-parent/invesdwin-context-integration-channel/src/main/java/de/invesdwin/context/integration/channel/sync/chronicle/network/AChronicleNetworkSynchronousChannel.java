package de.invesdwin.context.integration.channel.sync.chronicle.network;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.chronicle.network.type.ChronicleSocketChannelType;
import de.invesdwin.util.time.duration.Duration;
import net.openhft.chronicle.network.tcp.ChronicleServerSocket;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocket;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

@NotThreadSafe
public abstract class AChronicleNetworkSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected ChronicleSocket socket;
    protected ChronicleSocketChannel socketChannel;
    protected ChronicleSocketChannelType type;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    protected ChronicleServerSocketChannel serverSocketChannel;
    protected ChronicleServerSocket serverSocket;

    public AChronicleNetworkSynchronousChannel(final ChronicleSocketChannelType type,
            final InetSocketAddress socketAddress, final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocketChannel = type.newServerSocketChannel(socketAddress.toString());
            serverSocketChannel.bind(socketAddress);
            serverSocket = serverSocketChannel.socket();
            socketChannel = type.acceptSocketChannel(serverSocketChannel);
            socket = socketChannel.socket();
        } else {
            for (int tries = 0;; tries++) {
                socketChannel = type.newSocketChannel(SocketChannel.open());
                try {
                    socketChannel.connect(socketAddress);
                    socket = socketChannel.socket();
                    break;
                } catch (final ConnectException e) {
                    socketChannel.close();
                    socketChannel = null;
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
        //non-blocking sockets are a bit faster than blocking ones
        socketChannel.configureBlocking(false);
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
        socket = null;
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
