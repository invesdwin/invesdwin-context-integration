package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;

@Immutable
public class ClosedSynchronousWriter<M> implements ISynchronousWriter<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousWriter INSTANCE = new ClosedSynchronousWriter<>();
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final SynchronousWriterSpinWait SPIN_WAIT = new SynchronousWriterSpinWait<>(INSTANCE);

    @SuppressWarnings("unchecked")
    public static final <T> ClosedSynchronousWriter<T> getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    public static <T> SynchronousWriterSpinWait<T> getSpinWait() {
        return SPIN_WAIT;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void open() throws IOException {}

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final M message) throws IOException {}

    @Override
    public boolean writeFlushed() {
        return true;
    }
}
