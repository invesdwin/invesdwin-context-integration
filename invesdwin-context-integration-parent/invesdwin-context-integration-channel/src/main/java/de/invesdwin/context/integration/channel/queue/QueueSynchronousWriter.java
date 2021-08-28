package de.invesdwin.context.integration.channel.queue;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class QueueSynchronousWriter<M> implements ISynchronousWriter<M> {

    private Queue<IReference<M>> queue;

    public QueueSynchronousWriter(final Queue<IReference<M>> queue) {
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
        queue.add(EmptyReference.getInstance());
        queue = null;
    }

    @Override
    public void write(final M message) throws IOException {
        queue.add(ImmutableReference.of(message));
    }

}
