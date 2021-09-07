package de.invesdwin.context.integration.channel.async;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;

@NotThreadSafe
public class SyncAsynchronousHandler<I, O> implements IAsynchronousHandler<I, O> {

    private final ISynchronousReader<I> inputReader;
    private final ISynchronousWriter<O> outputWriter;

    public SyncAsynchronousHandler(final ISynchronousReader<I> inputReader, final ISynchronousWriter<O> outputWriter) {
        this.inputReader = inputReader;
        this.outputWriter = outputWriter;
    }

    @Override
    public O open() throws IOException {

        return null;
    }

    @Override
    public O handle(final I input) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        inputReader.close();
        outputWriter.close();
    }

}
