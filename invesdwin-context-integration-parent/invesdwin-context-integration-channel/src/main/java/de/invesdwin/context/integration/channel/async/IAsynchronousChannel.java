package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;

public interface IAsynchronousChannel extends Closeable {

    void open();

    @Override
    void close();

    boolean isClosed();

}
