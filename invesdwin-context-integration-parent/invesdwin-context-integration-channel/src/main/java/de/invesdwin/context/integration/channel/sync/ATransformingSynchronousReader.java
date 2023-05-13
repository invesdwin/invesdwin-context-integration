package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public abstract class ATransformingSynchronousReader<I, O> implements ISynchronousReader<O> {

    private final ISynchronousReader<I> input;

    public ATransformingSynchronousReader(final ISynchronousReader<I> input) {
        this.input = input;
    }

    @Override
    public void open() throws IOException {
        input.open();
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return input.hasNext();
    }

    @Override
    public O readMessage() throws IOException {
        return transform(input.readMessage());
    }

    protected abstract O transform(I message);

    @Override
    public void readFinished() throws IOException {
        input.readFinished();
    }

}
