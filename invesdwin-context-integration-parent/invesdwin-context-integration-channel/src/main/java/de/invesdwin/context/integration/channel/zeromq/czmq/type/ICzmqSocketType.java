package de.invesdwin.context.integration.channel.zeromq.czmq.type;

import org.zeromq.SocketType;

public interface ICzmqSocketType {

    SocketType getWriterSocketType();

    SocketType getReaderSocketType();

}
