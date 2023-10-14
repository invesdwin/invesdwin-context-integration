package de.invesdwin.context.integration.channel.rpc.base.endpoint.session;

import java.io.Closeable;

public interface ISynchronousEndpointSessionFactory extends Closeable {

    ISynchronousEndpointSession newSession();

    @Override
    default void close() {}

}
