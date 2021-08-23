package de.invesdwin.context.integration.channel.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;

@NotThreadSafe
public class BlockingQueueSynchronousWriter<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousWriter<M> {

    public BlockingQueueSynchronousWriter(final BlockingQueue<ISynchronousCommand<M>> queue) {
        super(queue);
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        final ISynchronousCommand<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousCommand.getInstance()) {
                throw new IllegalStateException("Multiple writers on queue are not supported!");
            } else {
                close();
                return;
            }
        }

        try {
            queue.put(new ImmutableSynchronousCommand<M>(type, sequence, message));
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        final ISynchronousCommand<M> closedMessage = queue.poll();
        if (closedMessage != null) {
            if (closedMessage != EmptySynchronousCommand.getInstance()) {
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
