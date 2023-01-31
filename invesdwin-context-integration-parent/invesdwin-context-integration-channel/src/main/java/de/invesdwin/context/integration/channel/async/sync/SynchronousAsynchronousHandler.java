package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.time.duration.Duration;

/**
 * Turn two synchronous channels into a handler.
 */
@NotThreadSafe
public class SynchronousAsynchronousHandler<I, O> implements IAsynchronousHandler<I, O> {

    private final ISynchronousReader<O> outputReader;
    private final ISynchronousWriter<I> inputWriter;
    private final ASpinWait writerSpinWait;
    private final Duration timeout;

    /**
     * Wrap inputReader in a BlockingSynchronousReader if a response should be guaranteed.
     */
    public SynchronousAsynchronousHandler(final ISynchronousReader<O> outputReader,
            final ISynchronousWriter<I> inputWriter, final Duration timeout) {
        this.outputReader = outputReader;
        this.inputWriter = inputWriter;
        this.writerSpinWait = newSpinWait(inputWriter);
        this.timeout = timeout;
    }

    /**
     * Override this to disable spinning or configure type of waits.
     */
    protected ASpinWait newSpinWait(final ISynchronousWriter<I> delegate) {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return delegate.writeFinished();
            }
        };
    }

    @Override
    public O open() throws IOException {
        outputReader.open();
        inputWriter.open();
        if (outputReader.hasNext()) {
            final O message = outputReader.readMessage();
            outputReader.readFinished();
            return message;
        } else {
            return null;
        }
    }

    @Override
    public O handle(final I input) throws IOException {
        inputWriter.write(input);
        try {
            //maybe block here
            if (!writerSpinWait.awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Write message timeout exceeded: " + timeout);
            }
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }
        if (outputReader.hasNext()) {
            final O message = outputReader.readMessage();
            outputReader.readFinished();
            return message;
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        outputReader.close();
        inputWriter.close();
    }

}
