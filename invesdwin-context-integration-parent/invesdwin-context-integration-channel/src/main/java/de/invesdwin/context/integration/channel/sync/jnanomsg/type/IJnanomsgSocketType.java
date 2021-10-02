package de.invesdwin.context.integration.channel.sync.jnanomsg.type;

import nanomsg.AbstractSocket;

public interface IJnanomsgSocketType {

    AbstractSocket newWriterSocket();

    AbstractSocket newReaderSocket();

    boolean isPublishSubscribeTopic();

}
