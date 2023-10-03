package de.invesdwin.context.integration.channel.sync.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class SimpleReferenceSynchronousReader<M> implements ISynchronousReader<M> {

    private IMutableReference<M> reference;

    @SuppressWarnings("unchecked")
    public SimpleReferenceSynchronousReader(final IMutableReference<? extends M> reference) {
        this.reference = (IMutableReference<M>) reference;
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
        return reference.getAndSet(null);
    }

    @Override
    public void readFinished() {
        //noop
    }

}
