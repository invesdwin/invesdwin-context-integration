package de.invesdwin.context.integration.channel;

import java.io.IOException;

import de.invesdwin.context.integration.channel.message.ISynchronousMessage;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    ISynchronousMessage<M> readMessage() throws IOException;

}
