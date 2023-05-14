package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.time.duration.Duration;

/**
 * Turn two synchronous channels into a handler.
 */
@NotThreadSafe
public class SynchronousAsynchronousHandler<I, O> implements IAsynchronousHandler<I, O> {

    private final ISynchronousReader<O> outputReader;
    private final ISynchronousWriter<I> inputWriter;
    private final SynchronousWriterSpinWait<I> writerSpinWait;
    private final Duration timeout;
    private long lastHeartbeatNanos = System.nanoTime();

    /**
     * Wrap inputReader in a BlockingSynchronousReader if a response should be guaranteed.
     */
    public SynchronousAsynchronousHandler(final ISynchronousReader<O> outputReader,
            final ISynchronousWriter<I> inputWriter, final Duration timeout) {
        this.outputReader = outputReader;
        this.inputWriter = inputWriter;
        this.writerSpinWait = new SynchronousWriterSpinWait<I>(inputWriter);
        this.timeout = timeout;
    }

    @Override
    public O open() throws IOException {
        outputReader.open();
        inputWriter.open();
        if (outputReader.hasNext()) {
            return outputReader.readMessage();
        } else {
            return null;
        }
    }

    @Override
    public O idle() throws IOException {
        if (isHeartbeatTimeout()) {
            throw FastEOFException.getInstance("heartbeat timeout [%s] exceeded", timeout);
        }
        return null;
    }

    public boolean isHeartbeatTimeout() {
        return timeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    @Override
    public O handle(final I input) throws IOException {
        //maybe block here
        writerSpinWait.waitForWrite(input, timeout);
        if (outputReader.hasNext()) {
            lastHeartbeatNanos = System.nanoTime();
            return outputReader.readMessage();
        } else {
            return null;
        }
    }

    @Override
    public void outputFinished() throws IOException {
        outputReader.readFinished();
    }

    @Override
    public void close() throws IOException {
        outputReader.close();
        inputWriter.close();
    }

}
