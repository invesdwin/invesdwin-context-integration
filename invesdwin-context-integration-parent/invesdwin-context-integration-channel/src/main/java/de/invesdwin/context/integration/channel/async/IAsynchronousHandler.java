package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

public interface IAsynchronousHandler<I, O> extends Closeable {

    O open() throws IOException;

    O handle(I input) throws IOException;

    @Override
    void close();

}
