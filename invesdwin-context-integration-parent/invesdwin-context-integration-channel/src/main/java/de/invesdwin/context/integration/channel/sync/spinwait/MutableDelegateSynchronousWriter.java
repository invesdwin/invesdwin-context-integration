package de.invesdwin.context.integration.channel.sync.spinwait;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;

@NotThreadSafe
public class MutableDelegateSynchronousWriter<M> implements ISynchronousWriter<M> {

    private ISynchronousWriter<M> delegate;

    public void setDelegate(final ISynchronousWriter<M> delegate) {
        this.delegate = delegate;
    }

    public ISynchronousWriter<M> getDelegate() {
        return delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public boolean writeReady() throws IOException {
        return delegate.writeReady();
    }

    @Override
    public void write(final M message) throws IOException {
        delegate.write(message);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return delegate.writeFlushed();
    }

}
