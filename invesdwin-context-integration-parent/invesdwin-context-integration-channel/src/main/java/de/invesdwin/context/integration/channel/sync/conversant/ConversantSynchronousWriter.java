package de.invesdwin.context.integration.channel.sync.conversant;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class ConversantSynchronousWriter<M> implements ISynchronousWriter<M> {

    private ConcurrentQueue<IReference<M>> queue;

    @SuppressWarnings("unchecked")
    public ConversantSynchronousWriter(final ConcurrentQueue<? extends IReference<M>> queue) {
        this.queue = (ConcurrentQueue<IReference<M>>) queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue.offer(newEmptyReference());
        queue = null;
    }

    protected IReference<M> newEmptyReference() {
        return EmptyReference.getInstance();
    }

    @Override
    public void write(final M message) throws IOException {
        queue.offer(newReference(message));
    }

    @Override
    public boolean writeFinished() throws IOException {
        return true;
    }

    protected IReference<M> newReference(final M message) {
        return ImmutableReference.of(message);
    }

}
