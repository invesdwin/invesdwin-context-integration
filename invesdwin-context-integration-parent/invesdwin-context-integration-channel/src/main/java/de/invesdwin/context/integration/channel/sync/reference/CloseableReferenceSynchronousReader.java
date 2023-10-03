package de.invesdwin.context.integration.channel.sync.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.error.FastEOFException;

@NotThreadSafe
public class CloseableReferenceSynchronousReader<M> implements ISynchronousReader<M> {

    private IMutableReference<IReference<M>> reference;

    @SuppressWarnings("unchecked")
    public CloseableReferenceSynchronousReader(final IMutableReference<? extends IReference<M>> reference) {
        this.reference = (IMutableReference<IReference<M>>) reference;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        reference = DisabledReference.getInstance();
    }

    @Override
    public boolean hasNext() throws IOException {
        return reference.get() != null;
    }

    @Override
    public M readMessage() throws IOException {
        final IReference<M> holder = reference.getAndSet(null);
        final M message = holder.get();
        if (message == null) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

}
