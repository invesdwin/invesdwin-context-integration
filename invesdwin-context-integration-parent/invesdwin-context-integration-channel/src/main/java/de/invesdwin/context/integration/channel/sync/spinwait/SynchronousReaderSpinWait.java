package de.invesdwin.context.integration.channel.sync.spinwait;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastTimeoutException;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SynchronousReaderSpinWait<M> {

    private final ISynchronousReader<M> reader;
    private final ASpinWait hasNext = newHasNext();

    public SynchronousReaderSpinWait(final ISynchronousReader<M> reader) {
        this.reader = reader;
    }

    public ISynchronousReader<M> getReader() {
        return reader;
    }

    public ASpinWait hasNext() {
        return hasNext;
    }

    private ASpinWait newHasNext() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return reader.hasNext();
            }
        };
    }

    public M waitForRead(final Duration timeout) throws IOException {
        try {
            final long startNanos = System.nanoTime();
            while (!hasNext().awaitFulfill(startNanos, timeout)) {
                onTimeout("Read message hasNext timeout exceeded", timeout, startNanos);
            }
            return reader.readMessage();
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

    protected void onTimeout(final String reason, final Duration timeout, final long startNanos)
            throws TimeoutException {
        throw FastTimeoutException.getInstance("%s: %s", reason, timeout);
    }
}
