package de.invesdwin.context.integration.channel.zeromq.czmq.type;

public interface ICzmqSocketType {

    ICzmqSocketFactory newWriterSocketFactory();

    ICzmqSocketFactory newReaderSocketFactory();

}
