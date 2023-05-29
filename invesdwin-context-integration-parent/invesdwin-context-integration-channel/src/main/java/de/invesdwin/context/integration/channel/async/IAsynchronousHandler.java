package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

public interface IAsynchronousHandler<I, O> extends Closeable {

    O open(IAsynchronousHandlerContext<O> context) throws IOException;

    /**
     * Can throw IOException/EOFException here if the connection should be closed due to a heartbeat timeout.
     */
    O idle(IAsynchronousHandlerContext<O> context) throws IOException;

    O handle(IAsynchronousHandlerContext<O> context, I input) throws IOException;

    /**
     * Needs to be called after open/idle/handle to indicate the output is not needed anymore and can be freed up (even
     * if the output was null).
     */
    void outputFinished(IAsynchronousHandlerContext<O> context) throws IOException;

}
