package de.invesdwin.context.integration.channel.sync.service.client.pool;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.service.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.sync.service.client.registry.ISynchronousEndpointClientSessionRegistry;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public final class SynchronousEndpointClientSessionPool extends ATimeoutObjectPool<SynchronousEndpointClientSession> {

    private final AtomicLong nextId = new AtomicLong(0);
    private final ISynchronousEndpointClientSessionRegistry registry;
    private final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory;

    /**
     * The endpointFactory can also return null endpoints if the ISynchronousEndpointClientSessionInfo creates the
     * transport from scratch based on the sessionId.
     */
    public SynchronousEndpointClientSessionPool(final ISynchronousEndpointClientSessionRegistry registry,
            final ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider> endpointFactory) {
        super(new Duration(10, FTimeUnit.MINUTES), Duration.ONE_MINUTE);
        this.registry = registry;
        this.endpointFactory = endpointFactory;
    }

    @Override
    public void invalidateObject(final SynchronousEndpointClientSession element) {
        element.close();
    }

    @Override
    protected SynchronousEndpointClientSession newObject() {
        return new SynchronousEndpointClientSession(this, registry, nextId.incrementAndGet(), endpointFactory);
    }

    @Override
    protected void passivateObject(final SynchronousEndpointClientSession element) {}

}
