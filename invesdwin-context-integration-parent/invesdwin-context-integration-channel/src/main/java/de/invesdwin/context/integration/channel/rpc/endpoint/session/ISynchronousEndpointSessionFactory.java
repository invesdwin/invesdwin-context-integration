package de.invesdwin.context.integration.channel.rpc.endpoint.session;

import java.io.Closeable;

public interface ISynchronousEndpointSessionFactory extends Closeable {

    ISynchronousEndpointSession newSession();

    @Override
    void close();

}
