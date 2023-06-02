package de.invesdwin.context.integration.channel.rpc.client.session.single;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;

@ThreadSafe
public class ClosingSinglexplexingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;

    public ClosingSinglexplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        this.endpointSessionFactory = endpointSessionFactory;
    }

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    public SingleplexingSynchronousEndpointClientSession borrowObject() {
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SingleplexingSynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    public void returnObject(final ISynchronousEndpointClientSession element) {
        invalidateObject(element);
    }

    @Override
    public void clear() {}

    @Override
    public void close() {}

}
