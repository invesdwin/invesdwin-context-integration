package de.invesdwin.context.integration.channel.conversant;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class ConversantSynchronousWriter<M> implements ISynchronousWriter<M> {

    private ConcurrentQueue<IReference<M>> queue;

    public ConversantSynchronousWriter(final ConcurrentQueue<IReference<M>> queue) {
        this.queue = queue;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        queue.offer(EmptyReference.getInstance());
        queue = null;
    }

    @Override
    public void write(final M message) throws IOException {
        queue.offer(ImmutableReference.of(message));
    }

}
