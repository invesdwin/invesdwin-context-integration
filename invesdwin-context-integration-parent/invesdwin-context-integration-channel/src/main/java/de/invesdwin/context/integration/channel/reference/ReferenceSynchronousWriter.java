package de.invesdwin.context.integration.channel.reference;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.DisabledReference;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IMutableReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.concurrent.reference.ImmutableReference;

@NotThreadSafe
public class ReferenceSynchronousWriter<M> implements ISynchronousWriter<M> {

    private IMutableReference<IReference<M>> reference;

    @SuppressWarnings("unchecked")
    public ReferenceSynchronousWriter(final IMutableReference<? extends IReference<M>> reference) {
        this.reference = (IMutableReference<IReference<M>>) reference;
    }

    @Override
    public void open() throws IOException {
    }

    @Override
    public void close() throws IOException {
        reference.set(newEmptyReference());
        reference = DisabledReference.getInstance();
    }

    protected DisabledReference<M> newEmptyReference() {
        return EmptyReference.getInstance();
    }

    @Override
    public void write(final M message) throws IOException {
        reference.set(newReference(message));
    }

    protected IReference<M> newReference(final M message) {
        return ImmutableReference.of(message);
    }

}
