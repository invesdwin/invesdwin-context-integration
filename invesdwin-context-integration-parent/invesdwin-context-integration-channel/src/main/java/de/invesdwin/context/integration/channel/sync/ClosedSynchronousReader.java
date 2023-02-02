package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.util.error.FastEOFException;

@Immutable
public class ClosedSynchronousReader<M> implements ISynchronousReader<M> {

    @SuppressWarnings("rawtypes")
    private static final ClosedSynchronousReader INSTANCE = new ClosedSynchronousReader<>();
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static final SynchronousReaderSpinWait SPIN_WAIT = new SynchronousReaderSpinWait<>(INSTANCE);

    @SuppressWarnings("unchecked")
    public static <T> ClosedSynchronousReader<T> getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    public static <T> SynchronousReaderSpinWait<T> getSpinWait() {
        return SPIN_WAIT;
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
        throw FastEOFException.getInstance("always closed");
    }
}
