package de.invesdwin.context.integration.channel.zeromq.czmq;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeromq.czmq.Zsock;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.context.integration.channel.zeromq.ZeromqSocketTypes;
import de.invesdwin.context.integration.channel.zeromq.czmq.type.ICzmqSocketFactory;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public abstract class ACzmqSynchronousChannel implements ISynchronousChannel {

    private static final int TYPE_INDEX = 0;
    private static final int TYPE_SIZE = Integer.BYTES;

    private static final int SEQUENCE_INDEX = TYPE_INDEX + TYPE_SIZE;
    private static final int SEQUENCE_SIZE = Integer.BYTES;

    private static final int MESSAGE_INDEX = SEQUENCE_INDEX + SEQUENCE_SIZE;

    protected Zsock socket;

    protected final ICzmqSocketFactory socketFactory;
    protected final String addr;
    protected final boolean server;
    protected byte[] topic = Bytes.EMPTY_ARRAY;
    protected int typeIndex = -1;
    protected int sequenceIndex = -1;
    protected int messageIndex = -1;

    public ACzmqSynchronousChannel(final ICzmqSocketFactory socketFactory, final String addr, final boolean server) {
        this.socketFactory = socketFactory;
        this.addr = addr;
        this.server = server;
    }

    @Override
    public void open() throws IOException {
        socket = socketFactory.newSocket(addr, getPublishSubscribeTopic());
        if (socket.self == 0) {
            throw new IOException("unable to create socket: " + addr);
        }
        socket.setConnectTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setHeartbeatTimeout(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setSndtimeo(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setRcvtimeo(ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        //connect/bind is handled by czmq as it seems
        //        if (!addr.startsWith("inproc:")) {
        //            if (server) {
        //                final int bind = socket.bind(addr);
        //                if (bind < 0) {
        //                    throw new BindException("Unable to bind to: " + addr);
        //                }
        //            } else {
        //                for (int tries = 0;; tries++) {
        //                    final int connect = socket.connect(addr);
        //                    if (connect < 0) {
        //                        if (tries < getMaxConnectRetries()) {
        //                            try {
        //                                getConnectRetryDelay().sleep();
        //                            } catch (final InterruptedException e1) {
        //                                throw new RuntimeException(e1);
        //                            }
        //                        } else {
        //                            throw new ConnectException("Unable to connect to: " + addr);
        //                        }
        //                    } else {
        //                        break;
        //                    }
        //                }
        //            }
        //        }
        final int socketType = socketFactory.getSocketType();
        if (socketType == ZeromqSocketTypes.SUB || socketType == ZeromqSocketTypes.XSUB) {
            topic = getPublishSubscribeTopic().getBytes();
        } else if (socketType == ZeromqSocketTypes.PUB || socketType == ZeromqSocketTypes.XPUB) {
            topic = getPublishSubscribeTopic().getBytes();
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
