package de.invesdwin.context.integration.channel;

import java.io.IOException;

import de.invesdwin.context.integration.channel.command.ISynchronousCommand;

public interface ISynchronousReader<M> extends ISynchronousChannel {

    boolean hasNext() throws IOException;

    ISynchronousCommand<M> readMessage() throws IOException;

}
