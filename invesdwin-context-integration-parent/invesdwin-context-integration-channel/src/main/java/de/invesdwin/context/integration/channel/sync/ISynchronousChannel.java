package de.invesdwin.context.integration.channel.sync;

import java.io.Closeable;
import java.io.IOException;

public interface ISynchronousChannel extends Closeable {

    /**
     * Channels should be opened in a different (acceptor) thread than a multiplexing thread (so that reads/writes work
     * as efficient as possible). Open can block during handshake operations (in order to validate keys/secrets eager to
     * prevent denial of service attacks on the multiplexer thread). For anyhow blocking read/write the same thread can
     * be used.
     */
    void open() throws IOException;

    /**
     * Closing should also be done in a different thread than a multiplexing thread, same as open because it might block
     * to send to termination message to the other side that needs to be ACKnowledged.
     */
    @Override
    void close() throws IOException;

}
