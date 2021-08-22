package de.invesdwin.context.integration.channel.nio;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.socket.udp.ADatagramSocketSynchronousChannel;
import de.invesdwin.context.integration.serde.ISerde;
import de.invesdwin.context.integration.serde.basic.IntegerSerde;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ASocketChannelSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_POS = 0;
    public static final ISerde<Integer> TYPE_SERDE = IntegerSerde.GET;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;
    public static final byte TYPE_CLOSED_VALUE = -1;

    public static final int SEQUENCE_POS = TYPE_POS + TYPE_OFFSET;
    public static final ISerde<Integer> SEQUENCE_SERDE = TYPE_SERDE;
    public static final int SEQUENCE_OFFSET = TYPE_OFFSET;
    public static final byte SEQUENCE_CLOSED_VALUE = -1;

    public static final int SIZE_POS = SEQUENCE_POS + SEQUENCE_OFFSET;
    public static final ISerde<Integer> SIZE_SERDE = SEQUENCE_SERDE;
    public static final int SIZE_OFFSET = SEQUENCE_OFFSET;

    public static final int MESSAGE_POS = SIZE_POS + SIZE_OFFSET;

    protected final int estimatedMaxMessageSize;
    protected final int bufferSize;
    protected SocketChannel socket;
    private final SocketAddress socketAddress;
    private final boolean server;

    public ASocketChannelSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.bufferSize = estimatedMaxMessageSize + MESSAGE_POS;
    }

    @Override
    public void open() throws IOException {
        socket = SocketChannel.open();
        final Socket underlying = socket.socket();
        underlying.setTcpNoDelay(true);
        underlying.setTrafficClass(
                ADatagramSocketSynchronousChannel.IPTOS_LOWDELAY | ADatagramSocketSynchronousChannel.IPTOS_THROUGHPUT);
        underlying.setReceiveBufferSize(bufferSize);
        underlying.setSendBufferSize(bufferSize);
        underlying.setTcpNoDelay(true);
        if (server) {
            socket.bind(socketAddress);
        } else {
            for (int tries = 0;; tries++) {
                try {
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
        //set non blocking after connection is established
        socket.configureBlocking(false);
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
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
