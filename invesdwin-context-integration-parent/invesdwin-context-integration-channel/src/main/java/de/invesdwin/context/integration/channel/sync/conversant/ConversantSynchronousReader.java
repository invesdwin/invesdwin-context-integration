package de.invesdwin.context.integration.channel.sync.conversant;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.conversantmedia.util.concurrent.ConcurrentQueue;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;

@NotThreadSafe
public class ConversantSynchronousReader<M> implements ISynchronousReader<M> {

    private ConcurrentQueue<IReference<M>> queue;

    @SuppressWarnings("unchecked")
    public ConversantSynchronousReader(final ConcurrentQueue<? extends IReference<M>> queue) {
        this.queue = (ConcurrentQueue<IReference<M>>) queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        queue = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return !queue.isEmpty();
    }

    @Override
    public M readMessage() throws IOException {
        final IReference<M> reference = queue.poll();
        final M message = reference.get();
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
