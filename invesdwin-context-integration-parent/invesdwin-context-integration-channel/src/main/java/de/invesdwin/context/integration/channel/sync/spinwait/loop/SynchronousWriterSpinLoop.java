package de.invesdwin.context.integration.channel.sync.spinwait.loop;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.FastTimeoutException;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SynchronousWriterSpinLoop<M> {

    private final ISynchronousWriter<M> writer;
    private final LoopInterruptedCheck loopInterruptedCheck = newLoopInterruptedCheck();

    public SynchronousWriterSpinLoop(final ISynchronousWriter<M> writer) {
        this.writer = writer;
    }

    public ISynchronousWriter<M> getWriter() {
        return writer;
    }

    protected LoopInterruptedCheck newLoopInterruptedCheck() {
        return new LoopInterruptedCheck(Duration.ONE_SECOND);
    }

    /**
     * This can be used when no actual communication is required, instead we spin for as long needed without any timed
     * spins or sleeps. E.g. to write all fragments to a complete buffer in a blocking RPC service.
     */
    public void spinForWrite(final M message, final Duration timeout) throws IOException {
        try {
            long startNanos = System.nanoTime();
            while (!writer.writeReady()) {
                if (loopInterruptedCheck.checkNoInterrupt()) {
                    if (timeout.isLessThanNanos(System.nanoTime() - startNanos)) {
                        onTimeout("Write message ready timeout exceeded", timeout, startNanos);
                    }
                }
                ASpinWait.onSpinWaitStatic();
            }
            writer.write(message);
            startNanos = System.nanoTime();
            while (!writer.writeFlushed()) {
                if (loopInterruptedCheck.checkNoInterrupt()) {
                    if (timeout.isLessThanNanos(System.nanoTime() - startNanos)) {
                        onTimeout("Write message flush timeout exceeded", timeout, startNanos);
                    }
                }
                ASpinWait.onSpinWaitStatic();
            }
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
