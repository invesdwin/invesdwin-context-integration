package de.invesdwin.context.integration.channel.sync.socket.udp;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.ABlockingDatagramSynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ADatagramSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected DatagramChannel socketChannel;
    protected DatagramSocket socket;

    public ADatagramSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;

    }

    @Override
    public void open() throws IOException {
        if (server) {
            socketChannel = DatagramChannel.open();
            socketChannel.bind(socketAddress);
            socket = socketChannel.socket();
        } else {
            for (int tries = 0;; tries++) {
                try {
                    socketChannel = DatagramChannel.open();
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
        //non-blocking datagrams are a lot faster than blocking ones
        socketChannel.configureBlocking(false);
        socket.setSendBufferSize(socketSize);
        socket.setReceiveBufferSize(socketSize);
        socket.setTrafficClass(ABlockingDatagramSynchronousChannel.IPTOS_LOWDELAY
                | ABlockingDatagramSynchronousChannel.IPTOS_THROUGHPUT);
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
    }

}
