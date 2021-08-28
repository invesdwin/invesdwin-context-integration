package de.invesdwin.context.integration.channel.queue;

import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.reference.IReference;

@NotThreadSafe
public class QueueSynchronousReader<M> implements ISynchronousReader<M> {

    private Queue<IReference<M>> queue;

    public QueueSynchronousReader(final Queue<IReference<M>> queue) {
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
    public M readMessage() throws IOException {
        final IReference<M> holder = queue.remove();
        final M message = holder.get();
        if (message == null) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
