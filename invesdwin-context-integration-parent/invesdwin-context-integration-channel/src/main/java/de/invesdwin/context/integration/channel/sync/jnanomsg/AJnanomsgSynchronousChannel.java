package de.invesdwin.context.integration.channel.sync.jnanomsg;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.jnanomsg.type.IJnanomsgSocketType;
import de.invesdwin.util.math.Bytes;
import nanomsg.AbstractSocket;
import nanomsg.Nanomsg;
import nanomsg.Nanomsg.SocketFlag;
import nanomsg.Nanomsg.SocketOption;
import nanomsg.pubsub.SubSocket;

@NotThreadSafe
public abstract class AJnanomsgSynchronousChannel implements ISynchronousChannel {

    public static final int FLAGS_DONTWAIT = SocketFlag.NN_DONTWAIT.value();
    public static final int ERRORS_EAGAIN = Nanomsg.Error.EAGAIN.value();

    protected AbstractSocket socket;

    protected final IJnanomsgSocketType socketType;
    protected final String addr;
    protected final boolean server;
    protected byte[] topic = Bytes.EMPTY_ARRAY;
    protected int messageIndex = -1;

    public AJnanomsgSynchronousChannel(final IJnanomsgSocketType socketType, final String addr, final boolean server) {
        this.socketType = socketType;
        this.addr = addr;
        this.server = server;
    }

    @Override
    public void open() throws IOException {
        socket = newSocket(socketType);
        socket.setSocketOpt(SocketOption.NN_RCVTIMEO, ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        socket.setSocketOpt(SocketOption.NN_SNDTIMEO, ContextProperties.DEFAULT_NETWORK_TIMEOUT_MILLIS);
        if (socket instanceof SubSocket) {
            topic = getPublishSubscribeTopic().getBytes();
            if (topic.length > 0) {
                socket.subscribe(topic);
            }
        }
        if (server) {
            socket.bind(addr);
        } else {
            socket.connect(addr);
        }
        updateIndexes();
    }

    protected abstract AbstractSocket newSocket(IJnanomsgSocketType socketType);

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

    @Override
    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

}
