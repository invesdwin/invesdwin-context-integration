package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.error.FastEOFException;

@Immutable
public class ClosedSynchronousReader<M> implements ISynchronousReader<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousReader INSTANCE = new ClosedSynchronousReader<>();

    public static <T> ClosedSynchronousReader<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void open() throws IOException {}

    @Override
    public M readMessage() {
        return null;
    }

    @Override
    public void readFinished() {
        //noop
    }

    @Override
    public boolean hasNext() throws IOException {
        throw FastEOFException.getInstance();
    }
}
