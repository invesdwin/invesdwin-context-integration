package de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;

@ThreadSafe
public final class MutableSessionlessHandlerContextPool extends AAgronaObjectPool<MutableSessionlessHandlerContext> {

    public static final MutableSessionlessHandlerContextPool INSTANCE = new MutableSessionlessHandlerContextPool();
    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";

    private MutableSessionlessHandlerContextPool() {
        super(newMaxPoolSize());
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(MutableSessionlessHandlerContextPool.class).getIntegerOptional(KEY_MAX_POOL_SIZE,
                ASynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL);
    }

    @Override
    protected MutableSessionlessHandlerContext newObject() {
        return new MutableSessionlessHandlerContext();
    }

    @Override
    protected boolean passivateObject(final MutableSessionlessHandlerContext element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final MutableSessionlessHandlerContext element) {
        element.clean();
    }

}
