package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;

@NotThreadSafe
public class ReadFinishedDelegateSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<M> delegate;
    private boolean readFinished;

    public ReadFinishedDelegateSynchronousReader(final ISynchronousReader<M> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        readFinished = true;
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public M readMessage() throws IOException {
        readFinished = false;
        return delegate.readMessage();
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
        readFinished = true;
    }

    public boolean isReadFinished() {
        return readFinished;
    }

}
