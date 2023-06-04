package de.invesdwin.context.integration.channel.rpc.client.session.multi;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient.ClientMethodInfo;
import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@ThreadSafe
public class MultipleMultiplexingSynchronousEndpointClientSessionPool
        implements ICloseableObjectPool<ISynchronousEndpointClientSession> {

    private final ISynchronousEndpointSessionFactory endpointSessionFactory;
    private final int maxSessions;
    private final int minThreadsPerSessionBeforeCreatingNewSession;
    private final List<Session> sessions;
    @GuardedBy("this")
    private volatile Future<?> scheduledFuture;

    public MultipleMultiplexingSynchronousEndpointClientSessionPool(
            final ISynchronousEndpointSessionFactory endpointSessionFactory, final int maxSessions,
            final int minThreadsPerSessionBeforeCreatingNewSession) {
        this.endpointSessionFactory = endpointSessionFactory;
        this.maxSessions = maxSessions;
        this.minThreadsPerSessionBeforeCreatingNewSession = minThreadsPerSessionBeforeCreatingNewSession;
        this.sessions = new ArrayList<>(maxSessions);
    }

    @Override
    public ISynchronousEndpointClientSession borrowObject() {
        return null;
    }

    @Override
    public void returnObject(final ISynchronousEndpointClientSession element) {

    }

    @Override
    public void clear() {}

    @Override
    public void invalidateObject(final ISynchronousEndpointClientSession element) {}

    @Override
    public void close() {}

    private static class Session implements ISynchronousEndpointClientSession {

        private final AtomicInteger refCount = new AtomicInteger();

        @Override
        public ICloseableByteBufferProvider request(final ClientMethodInfo methodInfo,
                final IByteBufferProvider request) {

            return null;
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

    }

}
