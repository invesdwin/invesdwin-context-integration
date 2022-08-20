package de.invesdwin.context.integration.channel.sync.aeron;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import io.aeron.Aeron;

@NotThreadSafe
public abstract class AAeronSynchronousChannel implements ISynchronousChannel {

    protected final AeronInstance instance;
    protected final String channel;
    protected final int streamId;
    protected Aeron aeron;

    public AAeronSynchronousChannel(final AeronInstance instance, final String channel, final int streamId) {
        this.instance = instance;
        this.channel = channel;
        this.streamId = streamId;
    }

    @Override
    public void open() throws IOException {
        this.aeron = instance.open();
    }

    @Override
    public void close() throws IOException {
        if (aeron != null) {
            instance.close(aeron);
            aeron = null;
        }
    }

}
