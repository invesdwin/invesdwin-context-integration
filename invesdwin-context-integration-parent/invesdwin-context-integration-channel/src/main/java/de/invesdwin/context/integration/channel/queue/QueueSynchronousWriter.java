package de.invesdwin.context.integration.channel.queue;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class QueueSynchronousWriter<M> implements ISynchronousWriter<M> {

    private Queue<ISynchronousCommand<M>> queue;

    public QueueSynchronousWriter(final Queue<ISynchronousCommand<M>> queue) {
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
        queue.add(EmptySynchronousCommand.getInstance());
        queue = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        queue.add(new ImmutableSynchronousCommand<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        queue.add(message);
    }

}
