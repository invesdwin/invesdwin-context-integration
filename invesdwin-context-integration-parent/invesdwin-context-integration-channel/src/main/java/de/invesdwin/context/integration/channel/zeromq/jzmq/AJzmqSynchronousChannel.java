package de.invesdwin.context.integration.channel.zeromq.jzmq;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.zeromq.ZeromqSocketTypes;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class AJzmqSynchronousChannel implements ISynchronousChannel {

    private static final int TYPE_INDEX = 0;
    private static final int TYPE_SIZE = Integer.BYTES;

    private static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    private static final int SEQUENCE_SIZE = Integer.BYTES;

    private static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    private static ZContext context;

    protected Socket socket;

    protected final int socketType;
    protected final String addr;
    protected final boolean server;
    protected byte[] topic = Bytes.EMPTY_ARRAY;
    protected int typeIndex = -1;
    protected int sequenceIndex = -1;
    protected int messageIndex = -1;

    public AJzmqSynchronousChannel(final int socketType, final String addr, final boolean server) {
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
        AJzmqSynchronousChannel.context = context;
    }

    public static ZContext newDefaultContext() {
        return new ZContext();
    }

    @Override
    public void open() throws IOException {
        final ZContext context = getContext();
        socket = context.createSocket(socketType);
        socket.setReceiveTimeOut(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setSendTimeOut(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        if (server) {
            socket.bind(addr);
        } else {
            socket.connect(addr);
        }
        if (socketType == ZeromqSocketTypes.SUB || socketType == ZeromqSocketTypes.XSUB) {
            topic = getPublishSubscribeTopic();
            socket.subscribe(topic);
        } else if (socketType == ZeromqSocketTypes.PUB || socketType == ZeromqSocketTypes.XPUB) {
            topic = getPublishSubscribeTopic();
        }
        updateIndexes();
    }

    private void updateIndexes() {
        final int topicSize = topic.length;
        typeIndex = topicSize + TYPE_INDEX;
        sequenceIndex = topicSize + SEQUENCE_INDEX;
        messageIndex = topicSize + MESSAGE_INDEX;
    }

    /**
     * Override with an actual value to set a topic for sending and receiving
     */
    protected byte[] getPublishSubscribeTopic() {
        return Bytes.EMPTY_ARRAY;
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
