package de.invesdwin.context.integration.channel.sync.nng.type;

import io.sisu.nng.NngException;
import io.sisu.nng.Socket;

public interface INngSocketType {

    Socket newWriterSocket() throws NngException;

    Socket newReaderSocket() throws NngException;

}
