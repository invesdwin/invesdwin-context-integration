package de.invesdwin.context.integration.channel;

import java.io.IOException;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    void write(M message) throws IOException;

}
