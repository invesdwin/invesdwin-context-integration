package de.invesdwin.context.integration.channel.conversant;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class ConversantSynchronousWriter<M> implements ISynchronousWriter<M> {

    private ConcurrentQueue<ISynchronousCommand<M>> queue;

    public ConversantSynchronousWriter(final ConcurrentQueue<ISynchronousCommand<M>> queue) {
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
        queue.offer(EmptySynchronousCommand.getInstance());
        queue = null;
    }

    @Override
    public void write(final int type, final int sequence, final M message) throws IOException {
        queue.offer(new ImmutableSynchronousCommand<M>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousCommand<M> message) throws IOException {
        queue.offer(message);
    }

}
