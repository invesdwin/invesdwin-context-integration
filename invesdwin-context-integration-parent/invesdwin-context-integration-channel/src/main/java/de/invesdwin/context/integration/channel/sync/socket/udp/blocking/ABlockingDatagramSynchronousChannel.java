package de.invesdwin.context.integration.channel.sync.socket.udp.blocking;

import java.io.IOException;
import java.net.ConnectException;
import java.net.DatagramSocket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ABlockingDatagramSynchronousChannel implements ISynchronousChannel {

    public static final int IPTOS_LOWCOST = 0x02;
    public static final int IPTOS_RELIABILITY = 0x04;
    public static final int IPTOS_THROUGHPUT = 0x08;
    public static final int IPTOS_LOWDELAY = 0x10;

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final SocketAddress socketAddress;
    protected final boolean server;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected DatagramSocket socket;

    public ABlockingDatagramSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;

    }

    @Override
    public void open() throws IOException {
        if (server) {
            socket = new DatagramSocket(socketAddress);
        } else {
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while (true) {
                try {
                    socket = new DatagramSocket();
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
        socket.setSendBufferSize(socketSize);
        socket.setReceiveBufferSize(socketSize);
        socket.setTrafficClass(IPTOS_LOWDELAY | IPTOS_THROUGHPUT);
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
    }

}
