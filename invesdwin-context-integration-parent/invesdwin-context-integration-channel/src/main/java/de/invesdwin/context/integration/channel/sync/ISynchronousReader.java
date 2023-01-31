package de.invesdwin.context.integration.channel.sync;

import java.io.IOException;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    /**
     * For non-blocking operation the message should be fully read when hasNext returns true. In the mean time other
     * readers or writers can perform their operations while waiting for more bytes/messages (for multiplexing).
     */
    boolean hasNext() throws IOException;

    /**
     * During blocking operation (non-multiplexing) an implementation might block this method until the message is fully
     * read.
     */
    M readMessage() throws IOException;

    /**
     * Call this when you are done with reading the message so that the message buffer can be freed/recycled.
     */
    void readFinished();

}
