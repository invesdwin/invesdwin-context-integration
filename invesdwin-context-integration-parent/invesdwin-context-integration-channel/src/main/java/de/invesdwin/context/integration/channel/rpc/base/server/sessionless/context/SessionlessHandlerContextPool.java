package de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.SynchronousEndpointServer;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;

@ThreadSafe
public final class SessionlessHandlerContextPool extends AAgronaObjectPool<SessionlessHandlerContext> {

    public static final SessionlessHandlerContextPool INSTANCE = new SessionlessHandlerContextPool();
    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";

    private SessionlessHandlerContextPool() {
        super(newMaxPoolSize());
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(SessionlessHandlerContextPool.class).getIntegerOptional(KEY_MAX_POOL_SIZE,
                SynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL);
    }

    @Override
    protected SessionlessHandlerContext newObject() {
        return new SessionlessHandlerContext();
    }

    @Override
    protected boolean passivateObject(final SessionlessHandlerContext element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final SessionlessHandlerContext element) {
        element.clean();
    }

}
