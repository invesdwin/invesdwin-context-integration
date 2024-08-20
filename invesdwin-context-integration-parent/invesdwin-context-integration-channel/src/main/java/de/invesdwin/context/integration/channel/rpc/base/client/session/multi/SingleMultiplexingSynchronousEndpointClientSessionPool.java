package de.invesdwin.context.integration.channel.rpc.base.client.session.multi;

import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.time.duration.Duration;

/**
 * This uses a given session only for one request at a time, opening more parallel connections when multiple requests
 * happen in parallel. This can be wasteful because an individual connection is not used to its fullest bandwidth but
 * can provide the lowest latency and overhead possible for single requests. Though for all other cases a multiplexing
 * client should be preferred.
 */
@ThreadSafe
public class SingleMultiplexingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private volatile boolean closed;
    private MultiplexingSynchronousEndpointClientSession singleSession;
    @GuardedBy("this")
    private volatile Future<?> scheduledFuture;

    public SingleMultiplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        this.endpointSessionFactory = endpointSessionFactory;
        ATimeoutObjectPool.ACTIVE_POOLS.incrementAndGet();
    }

    public boolean isActive() {
        return scheduledFuture != null;
    }

    public boolean isClosed() {
        return closed;
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
                        scheduledFuture = ATimeoutObjectPool.getScheduledExecutor()
                                .scheduleAtFixedRate(new CheckHeartbeatRunnable(), heartbeatInterval.longValue(),
                                        heartbeatInterval.longValue(), heartbeatInterval.getTimeUnit().timeUnitValue());
                    }
                }
            }
        }
        return singleSession;
    }

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {
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
    public void returnObject(final ISynchronousEndpointClientSession element) {
        //noop
    }

    @Override
    public void close() {
        if (!closed) {
            synchronized (this) {
                closed = true;
                clear();
                endpointSessionFactory.close();
                ATimeoutObjectPool.ACTIVE_POOLS.decrementAndGet();
                ATimeoutObjectPool.maybeCloseScheduledExecutor();
            }
        }
    }

    @Override
    public int size() {
        if (singleSession == null) {
            return 0;
        } else {
            return 1;
        }
    }

    private final class CheckHeartbeatRunnable implements Runnable {
        @Override
        public void run() {
            final MultiplexingSynchronousEndpointClientSession singleSessionCopy = singleSession;
            if (singleSessionCopy != null && singleSessionCopy.isHeartbeatTimeout()) {
                clear();
            }
        }
    }

}
