package de.invesdwin.context.integration.channel.sync.spinwait;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.loop.ASpinWait;

@NotThreadSafe
public class SynchronousWriterSpinWait extends ASpinWait {

    private ISynchronousWriter<?> writer;

    public SynchronousWriterSpinWait setWriter(final ISynchronousWriter<?> writer) {
        this.writer = writer;
        return this;
    }

    @Override
    public boolean isConditionFulfilled() throws Exception {
        return writer.writeFinished();
    }

}
