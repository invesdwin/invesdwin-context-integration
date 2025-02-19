package de.invesdwin.context.integration.channel.sync.spinwait.loop;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.FastTimeoutException;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SynchronousReaderSpinLoop<M> {

    private final ISynchronousReader<M> reader;
    private final LoopInterruptedCheck loopInterruptedCheck = newLoopInterruptedCheck();

    public SynchronousReaderSpinLoop(final ISynchronousReader<M> reader) {
        this.reader = reader;
    }

    public ISynchronousReader<M> getReader() {
        return reader;
    }

    protected LoopInterruptedCheck newLoopInterruptedCheck() {
        return new LoopInterruptedCheck(Duration.ONE_SECOND);
    }

    /**
     * This can be used when no actual communication is required, instead we spin for as long needed without any timed
     * spins or sleeps. E.g. to read all fragments from a complete buffer in a blocking RPC service.
     */
    public M spinForRead(final Duration timeout) throws IOException {
        try {
            final long startNanos = System.nanoTime();
            while (!reader.hasNext()) {
                if (loopInterruptedCheck.checkNoInterrupt()) {
                    if (timeout.isLessThanNanos(System.nanoTime() - startNanos)) {
                        onTimeout("Read message hasNext timeout exceeded", timeout, startNanos);
                    }
                }
                ASpinWait.onSpinWaitStatic();
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
