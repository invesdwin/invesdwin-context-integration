package de.invesdwin.context.integration.channel.sync.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class CloseableReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<IReference<M>> reference;

    @SuppressWarnings("unchecked")
    public CloseableReferenceSynchronousWriter(final IMutableReference<? extends IReference<M>> reference) {
        this.reference = (IMutableReference<IReference<M>>) reference;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        reference.set(newEmptyReference());
        reference = DisabledReference.getInstance();
    }

    protected DisabledReference<M> newEmptyReference() {
        return EmptyReference.getInstance();
    }

    @Override
    public boolean writeReady() throws IOException {
        return reference.get() == null;
    }

    @Override
    public void write(final M message) throws IOException {
        reference.set(newReference(message));
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

    protected IReference<M> newReference(final M message) {
        return ImmutableReference.of(message);
    }

}
