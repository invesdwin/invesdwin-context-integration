package de.invesdwin.context.integration.channel.rpc.client.session.single;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

/**
 * This uses use a given session only for one request at a time, opening more parallel connections when multiple
 * requests happen in parallel. This can be wasteful because an individual connection is not used to its fullest
 * bandwidth but can provide the lowest latency and overhead possible for single requests. Though for all other cases a
 * multiplexing client should be preferred.
 */
@ThreadSafe
public class SingleplexingSynchronousEndpointClientSessionPool
        extends ATimeoutObjectPool<ISynchronousEndpointClientSession>
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;

    public SingleplexingSynchronousEndpointClientSessionPool(
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
        return !closed && !element.isClosed();
    }

    @Override
    public void close() {
        super.close();
        endpointSessionFactory.close();
        closed = true;
    }

}
