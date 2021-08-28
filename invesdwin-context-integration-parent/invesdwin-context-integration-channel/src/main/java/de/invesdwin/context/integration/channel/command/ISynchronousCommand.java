package de.invesdwin.context.integration.channel.command;

import java.io.Closeable;

public interface ISynchronousCommand<M> extends Closeable {

    int getType();

    int getSequence();

    M getMessage();

}
