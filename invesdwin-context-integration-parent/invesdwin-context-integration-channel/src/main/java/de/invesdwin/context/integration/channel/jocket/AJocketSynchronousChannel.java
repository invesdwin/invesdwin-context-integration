package de.invesdwin.context.integration.channel.jocket;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.util.time.duration.Duration;
import jocket.net.JocketSocket;
import jocket.net.ServerJocket;

@NotThreadSafe
public abstract class AJocketSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected JocketSocket socket;
    private final int port;
    private final boolean server;
    private ServerJocket serverSocket;

    public AJocketSynchronousChannel(final int port, final boolean server, final int estimatedMaxMessageSize) {
        this.port = port;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        if (server) {
            serverSocket = new ServerJocket(port);
            socket = serverSocket.accept();
        } else {
            for (int tries = 0;; tries++) {
                try {
                    socket = new JocketSocket(port);
                    break;
                } catch (final ConnectException e) {
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
