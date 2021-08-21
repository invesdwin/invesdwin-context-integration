package de.invesdwin.context.integration.channel.zeromq;

import javax.annotation.concurrent.Immutable;

import org.zeromq.ZMQ;

@Immutable
public final class ZeromqSocketTypes {

    // Socket types, used when creating a Socket.
    /**
     * Flag to specify a exclusive pair of items.
     */
    public static final int PAIR = 0;
    /**
     * Flag to specify a PUB socket, receiving side must be a SUB or XSUB.
     */
    public static final int PUB = 1;
    /**
     * Flag to specify the receiving part of the PUB or XPUB socket.
     */
    public static final int SUB = 2;
    /**
     * Flag to specify a REQ socket, receiving side must be a REP.
     */
    public static final int REQ = 3;
    /**
     * Flag to specify the receiving part of a REQ socket.
     */
    public static final int REP = 4;
    /**
     * Flag to specify a DEALER socket (aka XREQ). DEALER is really a combined ventilator / sink that does
     * load-balancing on output and fair-queuing on input with no other semantics. It is the only socket type that lets
     * you shuffle messages out to N nodes and shuffle the replies back, in a raw bidirectional asynch pattern.
     */
    public static final int DEALER = 5;
    /**
     * Old alias for DEALER flag. Flag to specify a XREQ socket, receiving side must be a XREP.
     * 
     * @deprecated As of release 3.0 of zeromq, replaced by {@link #DEALER}
     */
    @Deprecated
    public static final int XREQ = DEALER;
    /**
     * Flag to specify ROUTER socket (aka XREP). ROUTER is the socket that creates and consumes request-reply routing
     * envelopes. It is the only socket type that lets you route messages to specific connections if you know their
     * identities.
     */
    public static final int ROUTER = 6;
    /**
     * Old alias for ROUTER flag. Flag to specify the receiving part of a XREQ socket.
     * 
     * @deprecated As of release 3.0 of zeromq, replaced by {@link #ROUTER}
     */
    @Deprecated
    public static final int XREP = ROUTER;
    /**
     * Flag to specify the receiving part of a PUSH socket.
     */
    public static final int PULL = 7;
    /**
     * Flag to specify a PUSH socket, receiving side must be a PULL.
     */
    public static final int PUSH = 8;
    /**
     * Flag to specify a XPUB socket, receiving side must be a SUB or XSUB. Subscriptions can be received as a message.
     * Subscriptions start with a '1' byte. Unsubscriptions start with a '0' byte.
     */
    public static final int XPUB = 9;
    /**
     * Flag to specify the receiving part of the PUB or XPUB socket. Allows
     */
    public static final int XSUB = 10;

    /**
     * Flag to specify a STREAMER device.
     */
    public static final int STREAMER = 1;

    /**
     * Flag to specify a FORWARDER device.
     */
    public static final int FORWARDER = 2;

    /**
     * Flag to specify a QUEUE device.
     */
    public static final int QUEUE = 3;

    /**
     * @see ZMQ#PULL
     */
    @Deprecated
    public static final int UPSTREAM = PULL;
    /**
     * @see ZMQ#PUSH
     */
    @Deprecated
    public static final int DOWNSTREAM = PUSH;

    private ZeromqSocketTypes() {
    }

}
