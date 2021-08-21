package de.invesdwin.context.integration.channel.zeromq;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ZeromqFlags {

    // Values for flags in Socket's send and recv functions.
    /**
     * Socket flag to indicate a nonblocking send or recv mode.
     */
    public static final int NOBLOCK = 1;
    public static final int DONTWAIT = 1;
    /**
     * Socket flag to indicate that more message parts are coming.
     */
    public static final int SNDMORE = 2;

    private ZeromqFlags() {
    }

}
