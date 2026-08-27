package de.invesdwin.context.integration.channel.jeromq.type;

import org.zeromq.api.SocketType;

public interface IJeromqSocketType {

    SocketType getWriterSocketType();

    SocketType getReaderSocketType();

}
