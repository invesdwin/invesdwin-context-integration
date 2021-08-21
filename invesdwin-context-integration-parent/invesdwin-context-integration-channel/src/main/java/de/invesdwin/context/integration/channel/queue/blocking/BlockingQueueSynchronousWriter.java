package de.invesdwin.context.integration.channel.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.message.EmptySynchronousMessage;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;

@NotThreadSafe
public class BlockingQueueSynchronousWriter<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousWriter<M> {

    public BlockingQueueSynchronousWriter(final BlockingQueue<ISynchronousMessage<M>> queue) {
        super(queue);
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final ISynchronousMessage<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousMessage.getInstance()) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(new ImmutableSynchronousMessage<M>(type, sequence, message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final ISynchronousMessage<M> message) throws IOException {
        final ISynchronousMessage<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousMessage.getInstance()) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(message);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
