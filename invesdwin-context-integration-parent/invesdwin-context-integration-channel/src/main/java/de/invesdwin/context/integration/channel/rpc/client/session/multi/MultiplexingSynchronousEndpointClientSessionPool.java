package de.invesdwin.context.integration.channel.rpc.client.session.multi;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.duration.Duration;

/**
 * This uses use a given session only for one request at a time, opening more parallel connections when multiple
 * requests happen in parallel. This can be wasteful because an individual connection is not used to its fullest
 * bandwidth but can provide the lowest latency and overhead possible for single requests. Though for all other cases a
 * multiplexing client should be preferred.
 */
@ThreadSafe
public class MultiplexingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;
    private MultiplexingSynchronousEndpointClientSession singleSession;
    @GuardedBy("this")
    private Future<?> scheduledFuture;

    public MultiplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        this.endpointSessionFactory = endpointSessionFactory;
        ATimeoutObjectPool.ACTIVE_POOLS.incrementAndGet();
    }

    private void maybeReconnect(final ISynchronousEndpointClientSession element) {
        final MultiplexingSynchronousEndpointClientSession cElement = (MultiplexingSynchronousEndpointClientSession) element;
        if (cElement.isClosed()) {
            cElement.close();
            synchronized (this) {
                if (cElement == singleSession) {
                    singleSession = null;
                }
            }
        }
    }

    @Override
    public void clear() {
        synchronized (this) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
            if (singleSession != null) {
                singleSession.close();
                singleSession = null;
            }
        }
    }

    @Override
    public ISynchronousEndpointClientSession borrowObject() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (singleSession == null) {
            synchronized (this) {
                if (closed) {
                    throw new IllegalStateException("closed");
                }
                if (singleSession == null) {
                    final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
                    singleSession = new MultiplexingSynchronousEndpointClientSession(endpointSession);
                    final Duration heartbeatInterval = endpointSession.getHeartbeatInterval();
                    if (scheduledFuture == null) {
                        scheduledFuture = ATimeoutObjectPool.getScheduledExecutor().scheduleAtFixedRate(() -> {
                            final MultiplexingSynchronousEndpointClientSession singleSessionCopy = singleSession;
                            if (singleSessionCopy != null && singleSessionCopy.isHeartbeatTimeout()) {
                                clear();
                            }
                        }, heartbeatInterval.longValue(), heartbeatInterval.longValue(),
                                heartbeatInterval.getTimeUnit().timeUnitValue());
                    }
                }
            }
        }
        return singleSession;
    }

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {
        maybeReconnect(element);
    }

    @Override
    public void returnObject(final ISynchronousEndpointClientSession element) {
        //noop
    }

    @Override
    public void close() {
        if (!closed) {
            synchronized (this) {
                closed = true;
                clear();
                ATimeoutObjectPool.ACTIVE_POOLS.decrementAndGet();
                ATimeoutObjectPool.maybeCloseScheduledExecutor();
            }
        }
    }

}
