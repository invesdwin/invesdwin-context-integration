package de.invesdwin.context.integration.channel.rpc.client.session;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class SynchronousEndpointClientSessionPool extends ATimeoutObjectPool<SynchronousEndpointClientSession>
        implements ICloseableObjectPool<SynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;

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
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    protected boolean passivateObject(final SynchronousEndpointClientSession element) {
        return !closed;
    }

    @Override
    public void close() {
        super.close();
        closed = true;
    }

}
