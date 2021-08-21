package de.invesdwin.context.integration.channel.zeromq.jeromq.type;

import org.zeromq.SocketType;

public interface IJeromqSocketType {

    SocketType getWriterSocketType();

    SocketType getReaderSocketType();

}
