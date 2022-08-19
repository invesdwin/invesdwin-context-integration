package de.invesdwin.context.integration.channel.sync.bufferingiterator;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;

@NotThreadSafe
public class BufferingIteratorSynchronousReader<M> implements ISynchronousReader<M> {

    private IBufferingIterator<IReference<M>> queue;

    @SuppressWarnings("unchecked")
    public BufferingIteratorSynchronousReader(final IBufferingIterator<? extends IReference<M>> queue) {
        this.queue = (IBufferingIterator<IReference<M>>) queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return queue.getHead() != null;
    }

    @Override
    public M readMessage() throws IOException {
        final IReference<M> holder = queue.next();
        final M message = holder.get();
        if (message == null) {
            close();
            throw new FastEOFException("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
