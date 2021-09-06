package de.invesdwin.context.integration.channel.sync.jeromq.type;

import org.zeromq.api.SocketType;

public interface IJeromqSocketType {

    SocketType getWriterSocketType();

    SocketType getReaderSocketType();

}
