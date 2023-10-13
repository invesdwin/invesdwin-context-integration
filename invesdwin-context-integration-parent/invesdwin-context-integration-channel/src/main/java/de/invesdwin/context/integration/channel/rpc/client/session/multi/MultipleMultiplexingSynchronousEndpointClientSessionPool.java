package de.invesdwin.context.integration.channel.rpc.client.session.multi;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.handler.ClientMethodInfo;
import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableList;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class MultipleMultiplexingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    public static final int DEFAULT_MAX_SESSIONS_COUNT = SynchronousEndpointServer.DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD;
    public static final int DEFAULT_CREATE_SESSION_REQUEST_THRESHOLD = SynchronousEndpointServer.DEFAULT_CREATE_IO_THREAD_SESSION_THRESHOLD;

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private final int maxSessionsCount;
    private final int createSessionRequestThreshold;
    @GuardedBy("sessions")
    private final IFastIterableList<Session> sessions;
    @GuardedBy("sessions")
    private volatile Future<?> scheduledFuture;
    private volatile boolean closed;
    private final ILock createSessionLock;

    public MultipleMultiplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory) {
        this.endpointSessionFactory = endpointSessionFactory;
        this.maxSessionsCount = newMaxSessionsCount();
        if (maxSessionsCount < 0) {
            throw new IllegalArgumentException(
                    "maxSessionsCount should be greater than or equal to 0: " + maxSessionsCount);
        }
        this.createSessionRequestThreshold = newCreateSessionRequestThreshold();
        if (createSessionRequestThreshold < 1) {
            throw new IllegalArgumentException("createSessionRequestThreshold should be greater than or equal to 1: "
                    + createSessionRequestThreshold);
        }
        this.sessions = ILockCollectionFactory.getInstance(true).newFastIterableArrayList(maxSessionsCount);
        this.createSessionLock = ILockCollectionFactory.getInstance(true)
                .newLock(MultipleMultiplexingSynchronousEndpointClientSessionPool.class.getSimpleName()
                        + "_CREATE_SESSION_LOCK");
        ATimeoutObjectPool.ACTIVE_POOLS.incrementAndGet();
    }

    protected int newMaxSessionsCount() {
        return DEFAULT_MAX_SESSIONS_COUNT;
    }

    protected int newCreateSessionRequestThreshold() {
        return DEFAULT_CREATE_SESSION_REQUEST_THRESHOLD;
    }

    public boolean isActive() {
        return scheduledFuture != null;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public ISynchronousEndpointClientSession borrowObject() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        maybeIncreaseSessionCount();
        return getSessionWithLeastRequests();
    }

    private ISynchronousEndpointClientSession getSessionWithLeastRequests() {
        final Session[] sessionsArray = sessions.asArray(Session.EMPTY_ARRAY);
        int minRequests = Integer.MAX_VALUE;
        int minRequestsIndex = 0;
        for (int i = 0; i < sessionsArray.length; i++) {
            final Session session = sessionsArray[i];
            if (session.isClosed()) {
                //already closed
                continue;
            }
            final int requests = session.requestsCount.get();
            if (requests == 0) {
                minRequests = 0;
                minRequestsIndex = i;
                break;
            }
            if (requests < minRequests) {
                minRequests = requests;
                minRequestsIndex = i;
            }
        }
        final Session session = sessionsArray[minRequestsIndex];
        session.requestsCount.incrementAndGet();
        return session;
    }

    private void maybeIncreaseSessionCount() {
        if (sessions.size() < maxSessionsCount) {
            if (sessions.size() == 0) {
                createSessionLock.lock();
                try {
                    increaseSessionCount();
                } finally {
                    createSessionLock.unlock();
                }
            } else if (createSessionLock.tryLock()) {
                try {
                    increaseSessionCount();
                } finally {
                    createSessionLock.unlock();
                }
            }
        }
    }

    private void increaseSessionCount() {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("closed");
            }
            if (sessions.size() < maxSessionsCount) {
                final Session[] sessionsArray = sessions.asArray(Session.EMPTY_ARRAY);
                for (int i = 0; i < sessionsArray.length; i++) {
                    final Session session = sessionsArray[i];
                    if (session.requestsCount.get() <= createSessionRequestThreshold) {
                        //no need to increase io runnables
                        return;
                    }
                }
                final ISynchronousEndpointSession endpointSession = endpointSessionFactory.newSession();
                final Session newSession = new Session(endpointSession);
                if (scheduledFuture == null) {
                    final Duration heartbeatInterval = endpointSession.getHeartbeatInterval();
                    scheduledFuture = ATimeoutObjectPool.getScheduledExecutor()
                            .scheduleAtFixedRate(new CheckHeartbeatRunnable(), heartbeatInterval.longValue(),
                                    heartbeatInterval.longValue(), heartbeatInterval.getTimeUnit().timeUnitValue());
                }
                sessions.add(newSession);
            }
        }
    }

    @Override
    public void returnObject(final ISynchronousEndpointClientSession element) {
        final Session cElement = (Session) element;
        cElement.requestsCount.decrementAndGet();
    }

    @Override
    public void clear() {
        synchronized (this) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
            for (int i = 0; i < sessions.size(); i++) {
                final Session session = sessions.get(i);
                Closeables.closeQuietly(session);
            }
            sessions.clear();
        }
    }

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {
        final Session cElement = (Session) element;
        if (cElement.isClosed()) {
            cElement.close();
            sessions.remove(cElement);
        }
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

    private final class CheckHeartbeatRunnable implements Runnable {
        @Override
        public void run() {
            final Session[] sessionsArray = sessions.asArray(Session.EMPTY_ARRAY);
            for (int i = 0; i < sessionsArray.length; i++) {
                final Session session = sessionsArray[i];
                if (session.isHeartbeatTimeout()) {
                    synchronized (this) {
                        if (sessions.size() == 1) {
                            clear();
                        } else {
                            sessions.remove(session);
                            session.close();
                        }
                    }
                }
            }
        }
    }

    private static final class Session implements ISynchronousEndpointClientSession {

        private static final Session[] EMPTY_ARRAY = new Session[0];

        private final AtomicInteger requestsCount = new AtomicInteger();
        private final MultiplexingSynchronousEndpointClientSession delegate;

        private Session(final ISynchronousEndpointSession endpointSession) {
            this.delegate = new MultiplexingSynchronousEndpointClientSession(endpointSession);
        }

        @Override
        public ICloseableByteBufferProvider request(final ClientMethodInfo methodInfo,
                final IByteBufferProvider request) {
            return delegate.request(methodInfo, request);
        }

        @Override
        public void close() {
            delegate.close();
        }

        public boolean isHeartbeatTimeout() {
            return delegate.isHeartbeatTimeout();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

    }

}
