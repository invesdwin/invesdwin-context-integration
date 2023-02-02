package de.invesdwin.context.integration.channel.sync.spinwait;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SynchronousWriterSpinWait<M> {

    private final ASpinWait writeReady = newWriteReady();
    private final ASpinWait writeFlushed = newWriteFlushed();

    private final ISynchronousWriter<M> writer;

    public SynchronousWriterSpinWait(final ISynchronousWriter<M> writer) {
        this.writer = writer;
    }

    public ISynchronousWriter<M> getWriter() {
        return writer;
    }

    public ASpinWait writeReady() {
        return writeReady;
    }

    public ASpinWait writeFlushed() {
        return writeFlushed;
    }

    private ASpinWait newWriteReady() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return writer.writeReady();
            }
        };
    }

    private ASpinWait newWriteFlushed() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return writer.writeFlushed();
            }
        };
    }

    public void waitForWrite(final M message, final Duration timeout) throws IOException {
        try {
            if (!writeReady().awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Write message ready timeout exceeded: " + timeout);
            }
            writer.write(message);
            if (!writeFlushed().awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Write message flush timeout exceeded: " + timeout);
            }
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }

}
