package de.invesdwin.context.integration.channel.rpc.server.blocking.context;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.blocking.ABlockingSynchronousEndpointServer;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public final class BlockingSychrounousEndpointServiceHandlerContextPool
        extends AAgronaObjectPool<BlockingSychrounousEndpointServiceHandlerContext> {

    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";
    private final ABlockingSynchronousEndpointServer parent;
    private final AtomicLong nextSessionId = new AtomicLong();

    public BlockingSychrounousEndpointServiceHandlerContextPool(final ABlockingSynchronousEndpointServer parent) {
        super(newMaxPoolSize());
        this.parent = parent;
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(BlockingSychrounousEndpointServiceHandlerContextPool.class).getIntegerOptional(
                KEY_MAX_POOL_SIZE, SynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL);
    }

    @Override
    protected BlockingSychrounousEndpointServiceHandlerContext newObject() {
        final String sessionId = String.valueOf(nextSessionId.incrementAndGet());
        final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler = parent.getHandlerFactory()
                .newHandler();
        return new BlockingSychrounousEndpointServiceHandlerContext(this, sessionId, handler);
    }

    @Override
    protected boolean passivateObject(final BlockingSychrounousEndpointServiceHandlerContext element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final BlockingSychrounousEndpointServiceHandlerContext element) {
        element.clean();
    }

}
