package de.invesdwin.context.integration.channel.async.sync;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class BlockingSynchronousWriter<M> implements ISynchronousWriter<M> {

    private final ISynchronousWriter<M> delegate;
    private final SynchronousWriterSpinWait<M> writerSpinWait;
    private final Duration timeout;

    public BlockingSynchronousWriter(final ISynchronousWriter<M> delegate, final Duration timeout) {
        this.delegate = delegate;
        this.writerSpinWait = new SynchronousWriterSpinWait<M>(delegate);
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
    public boolean writeReady() throws IOException {
        //always true because we block
        return true;
    }

    @Override
    public void write(final M message) throws IOException {
        writerSpinWait.waitForWrite(message, timeout);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        //always true because we blocked
        return true;
    }

}
