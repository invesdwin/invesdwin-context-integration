package de.invesdwin.context.integration.channel.sync.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;

@NotThreadSafe
public class SimpleReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<M> reference;

    @SuppressWarnings("unchecked")
    public SimpleReferenceSynchronousWriter(final IMutableReference<? extends M> reference) {
        this.reference = (IMutableReference<M>) reference;
    }

    public IMutableReference<M> getReference() {
        return reference;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        reference.set(null);
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
        reference.set(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
