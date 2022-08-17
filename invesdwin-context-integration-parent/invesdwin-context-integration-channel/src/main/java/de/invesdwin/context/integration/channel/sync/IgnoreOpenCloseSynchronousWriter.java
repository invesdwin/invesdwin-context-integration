package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class IgnoreOpenCloseSynchronousWriter<M> implements ISynchronousWriter<M> {

    private final ISynchronousWriter<M> delegate;

    private IgnoreOpenCloseSynchronousWriter(final ISynchronousWriter<M> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        //noop
    }

    @Override
    public void close() throws IOException {
        //noop
    }

    @Override
    public void write(final M message) throws IOException {
        delegate.write(message);
    }

    public static <T> IgnoreOpenCloseSynchronousWriter<T> valueOf(final ISynchronousWriter<T> delegate) {
        if (delegate instanceof IgnoreOpenCloseSynchronousWriter) {
            return (IgnoreOpenCloseSynchronousWriter<T>) delegate;
        } else {
            return new IgnoreOpenCloseSynchronousWriter<>(delegate);
        }
    }

}
