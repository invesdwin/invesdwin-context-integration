package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.util.collections.attributes.AttributesMap;

public interface IAsynchronousHandlerContext<O> extends Closeable {

    String getSessionId();

    AttributesMap getAttributes();

    /**
     * Can be used to write message asynchronously from outside of the handler thread (e.g. from a worker thread).
     */
    void write(O output) throws IOException;

}
