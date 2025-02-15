package de.invesdwin.context.integration.channel.rpc.base.server.session.result;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;

@ThreadSafe
public final class ProcessResponseResultPool extends AAgronaObjectPool<ProcessResponseResult> {

    public static final ProcessResponseResultPool INSTANCE = new ProcessResponseResultPool();
    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";

    private ProcessResponseResultPool() {
        super(newMaxPoolSize());
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(ProcessResponseResultPool.class).getIntegerOptional(KEY_MAX_POOL_SIZE,
                ASynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL);
    }

    @Override
    protected ProcessResponseResult newObject() {
        return new ProcessResponseResult();
    }

    @Override
    protected boolean passivateObject(final ProcessResponseResult element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final ProcessResponseResult element) {
        element.clean();
    }

}
