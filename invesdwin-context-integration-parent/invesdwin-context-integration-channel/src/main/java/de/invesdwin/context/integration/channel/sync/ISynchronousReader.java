package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    M readMessage() throws IOException;

    void readFinished();

}
