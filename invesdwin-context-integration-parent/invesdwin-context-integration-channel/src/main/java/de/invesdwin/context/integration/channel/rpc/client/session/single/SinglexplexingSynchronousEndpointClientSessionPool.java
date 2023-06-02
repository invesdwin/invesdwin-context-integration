package de.invesdwin.context.integration.channel.rpc.client.session.single;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class SinglexplexingSynchronousEndpointClientSessionPool
        extends ATimeoutObjectPool<ISynchronousEndpointClientSession>
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;

    public SinglexplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        super(new Duration(10, FTimeUnit.MINUTES), Duration.ONE_MINUTE);
        this.endpointSessionFactory = endpointSessionFactory;
    }

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    protected SingleplexingSynchronousEndpointClientSession newObject() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
        return new SingleplexingSynchronousEndpointClientSession(this, endpointSession);
    }

    @Override
    protected boolean passivateObject(final ISynchronousEndpointClientSession element) {
        return !closed;
    }

    @Override
    public void close() {
        super.close();
        closed = true;
    }

}
