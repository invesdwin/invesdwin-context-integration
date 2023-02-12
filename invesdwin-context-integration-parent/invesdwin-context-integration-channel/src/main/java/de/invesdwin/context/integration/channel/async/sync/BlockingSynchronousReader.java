package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BlockingSynchronousReader<M> implements ISynchronousReader<M> {

    private final ISynchronousReader<M> delegate;
    private final SynchronousReaderSpinWait<M> readerSpinWait;
    private final Duration timeout;

    public BlockingSynchronousReader(final ISynchronousReader<M> delegate, final Duration timeout) {
        this.delegate = delegate;
        this.readerSpinWait = new SynchronousReaderSpinWait<M>(delegate);
        this.timeout = timeout;
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        //always true since we just block internally
        return true;
    }

    @Override
    public M readMessage() throws IOException {
        //maybe block here
        return readerSpinWait.waitForRead(timeout);
    }

    @Override
    public void readFinished() throws IOException {
        delegate.readFinished();
    }

}
