package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;

@NotThreadSafe
public class BlockingSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<M> delegate;
    private final ASpinWait spinWait;

    public BlockingSynchronousReader(final ISynchronousReader<M> delegate) {
        this.delegate = delegate;
        this.spinWait = newSpinWait(delegate);
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    protected ASpinWait newSpinWait(final ISynchronousReader<M> delegate) {
        return new ASpinWait() {
            @Override
            protected boolean isConditionFulfilled() throws Exception {
                return delegate.hasNext();
            }
        };
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
        return true;
    }

    @Override
    public M readMessage() throws IOException {
        try {
            //maybe block here
            spinWait.awaitFulfill(System.nanoTime());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        return delegate.readMessage();
    }

}
