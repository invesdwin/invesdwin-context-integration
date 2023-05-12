package de.invesdwin.context.integration.channel.rpc.client.session;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;

@ThreadSafe
public final class ClosingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<SynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;

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
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    public void returnObject(final SynchronousEndpointClientSession element) {
        invalidateObject(element);
    }

    @Override
    public void clear() {}

    @Override
    public void close() {
        closed = true;
    }

}
