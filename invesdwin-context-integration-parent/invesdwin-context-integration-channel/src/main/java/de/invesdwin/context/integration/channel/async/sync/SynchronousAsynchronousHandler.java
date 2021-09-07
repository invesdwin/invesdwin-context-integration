package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;

/**
 * Turn two synchronous channels into a handler.
 */
@NotThreadSafe
public class SynchronousAsynchronousHandler<I, O> implements IAsynchronousHandler<I, O> {

    private final ISynchronousReader<O> outputReader;
    private final ISynchronousWriter<I> inputWriter;

    /**
     * Wrap inputReader in a BlockingSynchronousReader if a response should be guaranteed.
     */
    public SynchronousAsynchronousHandler(final ISynchronousReader<O> inputReader,
            final ISynchronousWriter<I> outputWriter) {
        this.outputReader = inputReader;
        this.inputWriter = outputWriter;
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
    public O handle(final I input) throws IOException {
        inputWriter.write(input);
        if (outputReader.hasNext()) {
            return outputReader.readMessage();
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
