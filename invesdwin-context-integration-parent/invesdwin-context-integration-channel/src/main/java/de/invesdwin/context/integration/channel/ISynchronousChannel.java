package de.invesdwin.context.integration.channel;

import java.io.Closeable;
import java.io.IOException;

public interface ISynchronousChannel extends Closeable {

    void open() throws IOException;

}
