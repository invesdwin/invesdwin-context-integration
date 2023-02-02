package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    /**
     * Might indicate backpressure or that the client did not yet read the message and another write would overwrite the
     * previous message.
     */
    boolean writeReady() throws IOException;

    void write(M message) throws IOException;

    /**
     * Non blocking write and trying to flush. Does not check for writeReady, thus might overwrite an existing message
     * that has not been read yet. Helpful to transmit a close/abort message.
     */
    default boolean writeAndFlushIfPossible(final M message) throws IOException {
        write(message);
        return writeFlushed();
    }

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
    boolean writeFlushed() throws IOException;

}
