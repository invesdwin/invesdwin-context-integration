package de.invesdwin.context.integration.channel.rpc.client.session;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.IObjectPool;

@ThreadSafe
public final class ClosedSynchronousEndpointClientSessionPool implements IObjectPool<SynchronousEndpointClientSession> {

    public static final ClosedSynchronousEndpointClientSessionPool INSTANCE = new ClosedSynchronousEndpointClientSessionPool();

    private ClosedSynchronousEndpointClientSessionPool() {}

    @Override
    public void invalidateObject(final SynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    public SynchronousEndpointClientSession borrowObject() {
        throw new UnsupportedOperationException("closed");
    }

    @Override
    public void returnObject(final SynchronousEndpointClientSession element) {
        invalidateObject(element);
    }

    @Override
    public void clear() {}

}
