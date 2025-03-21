package de.invesdwin.context.integration.channel.rpc.base.client;

import java.io.Closeable;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;

public interface IRpcSynchronousEndpointClient<T> extends Closeable {

    SerdeLookupConfig getSerdeLookupConfig();

    int getServiceId();

    Class<T> getServiceInterface();

    T getService();

    @Override
    void close();

    WrappedExecutorService getFutureExecutor();

    ICloseableObjectPool<ISynchronousEndpointClientSession> getSessionPool();

}
