package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;

public interface IAsynchronousHandler<I, O> extends Closeable {

    O open();

    O handle(I input);

    @Override
    void close();

}
