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

    @SuppressWarnings("unchecked")
    public QueueSynchronousWriter(final Queue<? extends IReference<M>> queue) {
        Assertions.assertThat(queue)
                .as("this implementation does not support non-blocking calls")
                .isNotInstanceOf(SynchronousQueue.class);
        this.queue = (Queue<IReference<M>>) queue;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        queue.add(newEmptyReference());
        queue = null;
    }

    protected IReference<M> newEmptyReference() {
        return EmptyReference.getInstance();
    }

    @Override
    public void write(final M message) throws IOException {
        queue.add(newReference(message));
    }

    protected IReference<M> newReference(final M message) {
        return ImmutableReference.of(message);
    }

}
