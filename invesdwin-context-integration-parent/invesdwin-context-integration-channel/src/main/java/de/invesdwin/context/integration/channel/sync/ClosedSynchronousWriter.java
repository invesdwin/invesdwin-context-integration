package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ClosedSynchronousWriter<M> implements ISynchronousWriter<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousWriter INSTANCE = new ClosedSynchronousWriter<>();

    @SuppressWarnings("unchecked")
    public static final <T> ClosedSynchronousWriter<T> getInstance() {
        return INSTANCE;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void open() throws IOException {}

    @Override
    public void write(final M message) throws IOException {}

    @Override
    public boolean writeFinished() {
        return true;
    }
}
