package de.invesdwin.context.integration.channel.bufferingiterator;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class BufferingIteratorSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IBufferingIterator<IReference<M>> queue;

    @SuppressWarnings("unchecked")
    public BufferingIteratorSynchronousWriter(final IBufferingIterator<? extends IReference<M>> queue) {
        this.queue = (IBufferingIterator<IReference<M>>) queue;
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
