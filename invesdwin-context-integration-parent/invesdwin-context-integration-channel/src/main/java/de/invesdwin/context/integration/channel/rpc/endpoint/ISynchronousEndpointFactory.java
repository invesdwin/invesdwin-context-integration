package de.invesdwin.context.integration.channel.rpc.endpoint;

import java.io.Closeable;

public interface ISynchronousEndpointFactory<R, W> extends Closeable {

    ISynchronousEndpoint<R, W> newEndpoint();

    @Override
    default void close() {}

}
