package de.invesdwin.context.integration.channel.sync.nng;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.nng.type.INngSocketType;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.duration.Duration;
import io.sisu.nng.NngException;
import io.sisu.nng.Socket;
import io.sisu.nng.pubsub.Sub0Socket;

@NotThreadSafe
public abstract class ANngSynchronousChannel implements ISynchronousChannel {

    private static final int SIZE_INDEX = 0;
    private static final int SIZE_SIZE = Integer.BYTES;

    private static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;

    protected Socket socket;

    protected final INngSocketType socketType;
    protected final String addr;
    protected final boolean server;
    protected byte[] topic = Bytes.EMPTY_ARRAY;
    protected int sizeIndex = -1;
    protected int messageIndex = -1;

    public ANngSynchronousChannel(final INngSocketType socketType, final String addr, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketType = socketType;
        this.addr = addr;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    @Override
    public void open() throws IOException {
        try {
            socket = newSocket(socketType);
            socket.setReceiveTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
            socket.setSendTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
            if (socket instanceof Sub0Socket) {
                final Sub0Socket cSocket = (Sub0Socket) socket;
                final String topicStr = getPublishSubscribeTopic();
                topic = topicStr.getBytes();
                if (topic.length > 0) {
                    cSocket.subscribe(topicStr);
                }
            }
            if (server) {
                socket.listen(addr);
            } else {
                socket.dial(addr);
            }
            updateIndexes();
        } catch (final NngException e) {
            throw new IOException(e);
        }
    }

    protected abstract Socket newSocket(INngSocketType socketType) throws NngException;

    private void updateIndexes() {
        final int topicSize = topic.length;
        sizeIndex = topicSize + SIZE_INDEX;
        messageIndex = MESSAGE_INDEX + topicSize;
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
            try {
                socket.close();
            } catch (final NngException e) {
                //ignore
            }
            socket = null;
        }
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
