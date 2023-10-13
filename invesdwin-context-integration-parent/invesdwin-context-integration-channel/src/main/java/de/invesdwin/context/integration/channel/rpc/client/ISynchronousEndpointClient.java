package de.invesdwin.context.integration.channel.rpc.client;

import java.io.Closeable;

import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;

public interface ISynchronousEndpointClient<T> extends Closeable {

    SerdeLookupConfig getSerdeLookupConfig();

    int getServiceId();

    Class<T> getServiceInterface();

    T getService();

    @Override
    void close();

}
