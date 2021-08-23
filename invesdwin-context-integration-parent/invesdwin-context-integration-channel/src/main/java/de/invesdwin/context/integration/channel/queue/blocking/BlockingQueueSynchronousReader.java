package de.invesdwin.context.integration.channel.queue.blocking;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;

@NotThreadSafe
public class BlockingQueueSynchronousReader<M> extends ABlockingQueueSynchronousChannel<M>
        implements ISynchronousReader<M> {
    ;

    private ISynchronousCommand<M> next;

    public BlockingQueueSynchronousReader(final BlockingQueue<ISynchronousCommand<M>> queue) {
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
    public ISynchronousCommand<M> readMessage() throws IOException {
        final ISynchronousCommand<M> message;
        message = next;
        next = null;
        if (message == EmptySynchronousCommand.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
