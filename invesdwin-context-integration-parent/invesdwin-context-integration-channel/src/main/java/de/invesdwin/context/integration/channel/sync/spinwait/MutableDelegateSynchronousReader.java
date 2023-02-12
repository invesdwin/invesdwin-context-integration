package de.invesdwin.context.integration.channel.sync.spinwait;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;

@NotThreadSafe
public class MutableDelegateSynchronousReader<M> implements ISynchronousReader<M> {

    private ISynchronousReader<M> delegate;

    public ISynchronousReader<M> getDelegate() {
        return delegate;
    }

    public void setDelegate(final ISynchronousReader<M> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
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

}
