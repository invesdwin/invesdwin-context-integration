package de.invesdwin.context.integration.channel.sync.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class BlockingQueueSynchronousWriter<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousWriter<M> {

    public BlockingQueueSynchronousWriter(final BlockingQueue<? extends IReference<M>> queue) {
        super(queue);
    }

    @Override
    public void write(final M message) throws IOException {
        final IReference<M> closedHolder = queue.poll();
        if (closedHolder != null) {
            if (closedHolder.get() != null) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(newReference(message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected IReference<M> newReference(final M message) {
        return ImmutableReference.of(message);
    }

}
