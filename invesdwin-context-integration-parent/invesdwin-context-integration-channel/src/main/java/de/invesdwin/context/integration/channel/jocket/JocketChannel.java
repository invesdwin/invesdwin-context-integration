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
public class JocketChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected JocketSocket socket;
    private final int port;
    private final boolean server;
    private ServerJocket serverSocket;

    private int openCount = 0;

    public JocketChannel(final int port, final boolean server, final int estimatedMaxMessageSize) {
        this.port = port;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    @Override
    public void open() throws IOException {
        openCount++;
        if (openCount > 1) {
            return;
        }
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
        openCount--;
        if (openCount > 0) {
            return;
        }
        if (socket != null) {
            socket.close();
            socket = null;
        }
        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
    }

    public JocketSocket getSocket() {
        return socket;
    }

    public EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
