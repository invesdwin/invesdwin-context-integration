package de.invesdwin.context.integration.channel.rpc.client.session;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.IObjectPool;

@ThreadSafe
public final class ClosingSynchronousEndpointClientSessionPool
        implements IObjectPool<SynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;

    public ClosingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        this.endpointSessionFactory = endpointSessionFactory;
    }

    @Override
    public void invalidateObject(final SynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    public SynchronousEndpointClientSession borrowObject() {
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    public void returnObject(final SynchronousEndpointClientSession element) {
        invalidateObject(element);
    }

    @Override
    public void clear() {}

}
