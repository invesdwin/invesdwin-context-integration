package de.invesdwin.context.integration.channel;

import java.io.IOException;

import de.invesdwin.context.integration.channel.message.ISynchronousMessage;

public interface ISynchronousWriter<M> extends ISynchronousChannel {

    void write(int type, int sequence, M message) throws IOException;

    void write(ISynchronousMessage<M> message) throws IOException;

}
