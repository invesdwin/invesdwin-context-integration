package de.invesdwin.context.integration.channel.rpc.client.session;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class SynchronousEndpointClientSessionPool extends ATimeoutObjectPool<SynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;

    public SynchronousEndpointClientSessionPool(final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        super(new Duration(10, FTimeUnit.MINUTES), Duration.ONE_MINUTE);
        this.endpointSessionFactory = endpointSessionFactory;
    }

    @Override
    public void invalidateObject(final SynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    protected SynchronousEndpointClientSession newObject() {
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    protected void passivateObject(final SynchronousEndpointClientSession element) {}

}
