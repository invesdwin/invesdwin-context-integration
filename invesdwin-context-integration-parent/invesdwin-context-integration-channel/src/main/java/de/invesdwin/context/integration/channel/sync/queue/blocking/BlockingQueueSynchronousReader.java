package de.invesdwin.context.integration.channel.sync.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;

/**
 * WARNING: can cause cpu spikes
 */
@Deprecated
@NotThreadSafe
public class BlockingQueueSynchronousReader<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousReader<M> {
    ;

    private IReference<M> next;

    public BlockingQueueSynchronousReader(final BlockingQueue<? extends IReference<M>> queue) {
        super(queue);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (next != null) {
            return true;
        }
        next = queue.poll();
        return next != null;
    }

    @Override
    public M readMessage() throws IOException {
        final IReference<M> holder;
        holder = next;
        next = null;
        final M message = holder.get();
        if (message == null) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
