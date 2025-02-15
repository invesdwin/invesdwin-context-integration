package de.invesdwin.context.integration.channel.async.mina;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.Immutable;

import org.apache.mina.core.future.WriteFuture;

@Immutable
public class WriteFutureAdapter implements Future<Void> {

    private final WriteFuture writeFuture;

    public WriteFutureAdapter(final WriteFuture delegate) {
        this.writeFuture = delegate;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return writeFuture.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        writeFuture.await();
        return null;
    }

    @Override
    public Void get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        writeFuture.await(timeout, unit);
        return null;
    }

    public static Future<?> valueOf(final WriteFuture writeFuture) {
        if (writeFuture == null) {
            return null;
        } else if (writeFuture instanceof Future) {
            return (Future<?>) writeFuture;
        } else {
            return new WriteFutureAdapter(writeFuture);
        }
    }

}
