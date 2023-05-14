package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

public interface IAsynchronousHandler<I, O> extends Closeable {

    O open() throws IOException;

    /**
     * Can throw IOException/EOFException here if the connection should be closed due to a heartbeat timeout.
     */
    O idle() throws IOException;

    O handle(I input) throws IOException;

    /**
     * Needs to be called after open or handle to indicate the output is not needed anymore and can be freed up (even if
     * the output was null).
     */
    void outputFinished() throws IOException;

}
