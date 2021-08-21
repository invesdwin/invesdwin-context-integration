package de.invesdwin.context.integration.channel.zeromq.czmq.type;

import org.zeromq.czmq.Zsock;

public interface ICzmqSocketFactory {

    Zsock newSocket(String endpoint, String topic);

    int getSocketType();

}
