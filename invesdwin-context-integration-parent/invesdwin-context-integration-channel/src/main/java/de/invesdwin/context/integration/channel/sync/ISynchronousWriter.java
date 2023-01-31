package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    void write(M message) throws IOException;

    /**
     * Repeatedly call this until the write is finished. Might return true immediately if the write happens blocking.
     * 
     * The method will try to write as much data as possible.
     * 
     * If an output buffer is full (or an interrupt happens or a destination is busy/locked or backpressure prevents
     * more writes right now), the method will return false so that the write can be retried after other write
     * operations were done in the mean time (e.g. during multiplexing, otherwise just loop until true is returned here
     * for blocking operation).
     * 
     * The method will return true when the message is fully written and the destination is ready to get a new message
     * written.
     */
    boolean writeFinished() throws IOException;

}
