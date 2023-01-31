package de.invesdwin.context.integration.channel.sync;

import java.io.Closeable;
import java.io.IOException;

public interface ISynchronousChannel extends Closeable {

    /**
     * Channels should be opened in a different (acceptor) thread than a multiplexing thread (so that reads/writes work
     * as efficient as possible). Open can block during handshake operations. For anyhow blocking read/write the same
     * thread can be used.
     */
    void open() throws IOException;

}
