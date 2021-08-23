package de.invesdwin.context.integration.channel.queue;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousReader<M> implements ISynchronousReader<M> {

    private Queue<ISynchronousCommand<M>> queue;

    public QueueSynchronousReader(final Queue<ISynchronousCommand<M>> queue) {
        Assertions.assertThat(queue)
                .as("this implementation does not support non-blocking calls")
                .isNotInstanceOf(SynchronousQueue.class);
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return queue.peek() != null;
    }

    @Override
    public ISynchronousCommand<M> readMessage() throws IOException {
        final ISynchronousCommand<M> message = queue.remove();
        if (message == EmptySynchronousCommand.getInstance()) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
