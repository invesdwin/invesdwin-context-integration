package de.invesdwin.context.integration.channel.zeromq.jeromq;

import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AJeromqSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_INDEX = 0;
    public static final int TYPE_SIZE = Integer.BYTES;

    public static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    public static final int SEQUENCE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private static ZContext context;

    protected Socket socket;

    private final SocketType socketType;
    private final String addr;
    private final boolean server;

    public AJeromqSynchronousChannel(final SocketType socketType, final String addr, final boolean server) {
        this.socketType = socketType;
        this.addr = addr;
        this.server = server;
    }

    public static synchronized ZContext getContext() {
        if (context == null) {
            context = newDefaultContext();
        }
        return context;
    }

    public static synchronized void setContext(final ZContext context) {
        AJeromqSynchronousChannel.context = context;
    }

    public static ZContext newDefaultContext() {
        return new ZContext(1);
    }

    @Override
    public void open() throws IOException {
        final ZContext context = getContext();
        socket = context.createSocket(socketType);
        socket.setReceiveTimeOut(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setSendTimeOut(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        if (server) {
            if (!socket.bind(addr)) {
                throw new BindException("Unable to bind to: " + addr);
            }
        } else {
            for (int tries = 0;; tries++) {
                if (socket.connect(addr)) {
                    break;
                } else {
                    if (tries < getMaxConnectRetries()) {
                        try {
                            getConnectRetryDelay().sleep();
                        } catch (final InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw new ConnectException("Unable to connect to: " + addr);
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
            if (server) {
                socket.unbind(addr);
            } else {
                socket.disconnect(addr);
            }
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
