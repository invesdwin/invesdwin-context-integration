package de.invesdwin.context.integration.channel.zeromq.type;

import org.zeromq.api.SocketType;

public interface IJeromqSocketType {

    SocketType getWriterSocketType();

    SocketType getReaderSocketType();

}
