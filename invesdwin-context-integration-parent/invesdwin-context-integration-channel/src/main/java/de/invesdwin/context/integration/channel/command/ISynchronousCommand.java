package de.invesdwin.context.integration.channel.command;

public interface ISynchronousCommand<M> {

    int getType();

    int getSequence();

    M getMessage();

}
