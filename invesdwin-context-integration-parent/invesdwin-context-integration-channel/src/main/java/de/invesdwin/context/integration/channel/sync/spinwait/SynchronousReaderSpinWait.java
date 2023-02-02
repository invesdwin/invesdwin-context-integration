package de.invesdwin.context.integration.channel.sync.spinwait;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.loop.ASpinWait;

@NotThreadSafe
public class SynchronousReaderSpinWait extends ASpinWait {

    private ISynchronousReader<?> reader;

    public SynchronousReaderSpinWait setReader(final ISynchronousReader<?> reader) {
        this.reader = reader;
        return this;
    }

    @Override
    public boolean isConditionFulfilled() throws Exception {
        return reader.hasNext();
    }

}
