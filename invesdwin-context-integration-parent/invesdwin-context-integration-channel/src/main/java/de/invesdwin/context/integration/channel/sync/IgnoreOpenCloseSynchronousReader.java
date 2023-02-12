package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class IgnoreOpenCloseSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<M> delegate;

    private IgnoreOpenCloseSynchronousReader(final ISynchronousReader<M> delegate) {
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
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public M readMessage() throws IOException {
        return delegate.readMessage();
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

    public static <T> IgnoreOpenCloseSynchronousReader<T> valueOf(final ISynchronousReader<T> delegate) {
        if (delegate instanceof IgnoreOpenCloseSynchronousReader) {
            return (IgnoreOpenCloseSynchronousReader<T>) delegate;
        } else {
            return new IgnoreOpenCloseSynchronousReader<>(delegate);
        }
    }

}
