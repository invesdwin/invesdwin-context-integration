package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWaitPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    void write(M message) throws IOException;

    /**
     * Non blocking write and trying to finish.
     */
    default boolean writeAndFinishIfPossible(final M message) throws IOException {
        write(message);
        return writeFinished();
    }

    /**
     * Uses the default network timeout to try to finish the write.
     */
    default boolean writeAndFinishBlocking(final M message) throws IOException, TimeoutException {
        try {
            writeAndFinishBlocking(message, ContextProperties.DEFAULT_NETWORK_TIMEOUT);
            return true;
        } catch (final TimeoutException e) {
            return false;
        }
    }

    /**
     * Tries to finish writing until the timeout is exceeded.
     */
    default void writeAndFinishBlocking(final M message, final Duration timeout) throws IOException, TimeoutException {
        if (!writeAndFinishIfPossible(message)) {
            final SynchronousWriterSpinWait spinWait = SynchronousWriterSpinWaitPool.INSTANCE.borrowObject()
                    .setWriter(this);
            final boolean success;
            try {
                success = spinWait.awaitFulfill(System.nanoTime(), timeout);
            } catch (final Exception e) {
                throw Throwables.propagate(e);
            } finally {
                SynchronousWriterSpinWaitPool.INSTANCE.returnObject(spinWait);
            }
            if (!success) {
                throw new TimeoutException("Write timeout exceeded: " + timeout);
            }
        }
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
    boolean writeFinished() throws IOException;

}
