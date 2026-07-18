package de.invesdwin.context.integration.channel.jnanomsg.type;

import nanomsg.AbstractSocket;

public interface IJnanomsgSocketType {

    AbstractSocket newWriterSocket();

    AbstractSocket newReaderSocket();

}
