package de.invesdwin.context.integration.channel.sync.jeromq;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.ContextFactory;
import org.zeromq.api.Context;
import org.zeromq.api.Socket;
import org.zeromq.api.SocketType;
import org.zeromq.jzmq.sockets.SocketBuilder;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AJeromqSynchronousChannel implements ISynchronousChannel {

    private static Context context;

    protected Socket socket;

    protected final SocketType socketType;
    protected final String addr;
    protected final boolean server;
    protected byte[] topic = Bytes.EMPTY_ARRAY;
    protected int messageIndex = -1;

    public AJeromqSynchronousChannel(final SocketType socketType, final String addr, final boolean server) {
        this.socketType = socketType;
        this.addr = addr;
        this.server = server;
    }

    public static synchronized Context getContext() {
        if (context == null) {
            context = newDefaultContext();
        }
        return context;
    }

    public static synchronized void setContext(final Context context) {
        AJeromqSynchronousChannel.context = context;
    }

    public static Context newDefaultContext() {
        return ContextFactory.createContext(1);
    }

    @Override
    public void open() throws IOException {
        final Context context = getContext();
        final SocketBuilder socketBuilder = context.buildSocket(socketType);
        socketBuilder.withReceiveTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socketBuilder.withSendTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        if (socketType == SocketType.SUB || socketType == SocketType.XSUB) {
            topic = getPublishSubscribeTopic().getBytes();
            socketBuilder.asSubscribable().subscribe(topic);
        } else if (socketType == SocketType.PUB || socketType == SocketType.XPUB) {
            topic = getPublishSubscribeTopic().getBytes();
        }
        if (server) {
            socket = socketBuilder.bind(addr);
        } else {
            socket = socketBuilder.connect(addr);
        }
        updateIndexes();
    }

    private void updateIndexes() {
        final int topicSize = topic.length;
        messageIndex = topicSize;
    }

    /**
     * Override with an actual value to set a topic for sending and receiving
     */
    protected String getPublishSubscribeTopic() {
        return "";
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
        final EOFException eof = new FastEOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
