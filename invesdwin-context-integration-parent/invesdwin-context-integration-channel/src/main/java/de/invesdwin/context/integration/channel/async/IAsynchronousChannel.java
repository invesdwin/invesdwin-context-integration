package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

public interface IAsynchronousChannel extends Closeable {

    void open() throws IOException;

    boolean isClosed();

}
