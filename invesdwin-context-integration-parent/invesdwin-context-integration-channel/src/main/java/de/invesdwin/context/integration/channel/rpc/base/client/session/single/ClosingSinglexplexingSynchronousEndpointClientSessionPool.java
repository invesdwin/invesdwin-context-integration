package de.invesdwin.context.integration.channel.rpc.base.client.session.single;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;

/**
 * Holds the session active only for a single call and closes it afterwards. This is useful for services where only
 * sporadic requests are made without the need to keep a connection alive for a longer time. For example in order to
 * negotiate a different (more secure or faster) transport.
 */
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
    public void close() {
        endpointSessionFactory.close();
    }

    @Override
    public int size() {
        return 0;
    }

}
