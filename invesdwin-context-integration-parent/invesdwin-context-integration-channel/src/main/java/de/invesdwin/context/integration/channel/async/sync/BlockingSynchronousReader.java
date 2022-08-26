package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BlockingSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<M> delegate;
    private final ASpinWait spinWait;
    private final Duration timeout;

    public BlockingSynchronousReader(final ISynchronousReader<M> delegate, final Duration timeout) {
        this.delegate = delegate;
        this.spinWait = newSpinWait(delegate);
        this.timeout = timeout;
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    protected ASpinWait newSpinWait(final ISynchronousReader<M> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
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
        //always true since we just block internally
        return true;
    }

    @Override
    public M readMessage() throws IOException {
        try {
            //maybe block here
            if (!spinWait.awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Read message timeout exceeded: " + timeout);
            }
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
        return delegate.readMessage();
    }

    @Override
    public void readFinished() {
        delegate.readFinished();
    }

}
