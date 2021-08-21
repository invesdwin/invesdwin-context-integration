package de.invesdwin.context.integration.channel.message;

public interface ISynchronousMessage<M> {

    int getType();

    int getSequence();

    M getMessage();

}
