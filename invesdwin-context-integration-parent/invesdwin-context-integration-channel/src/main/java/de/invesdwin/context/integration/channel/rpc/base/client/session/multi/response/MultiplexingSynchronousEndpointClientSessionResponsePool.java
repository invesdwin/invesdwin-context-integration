package de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.DefaultRpcSynchronousEndpointClient;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;

@ThreadSafe
public final class MultiplexingSynchronousEndpointClientSessionResponsePool
        extends AAgronaObjectPool<MultiplexingSynchronousEndpointClientSessionResponse> {

    public static final MultiplexingSynchronousEndpointClientSessionResponsePool INSTANCE = new MultiplexingSynchronousEndpointClientSessionResponsePool();
    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";

    private MultiplexingSynchronousEndpointClientSessionResponsePool() {
        super(newMaxPoolSize());
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(MultiplexingSynchronousEndpointClientSessionResponsePool.class)
                .getIntegerOptional(KEY_MAX_POOL_SIZE, DefaultRpcSynchronousEndpointClient.DEFAULT_MAX_PENDING_WORK_COUNT);
    }

    @Override
    protected MultiplexingSynchronousEndpointClientSessionResponse newObject() {
        return new MultiplexingSynchronousEndpointClientSessionResponse(this);
    }

    @Override
    protected boolean passivateObject(final MultiplexingSynchronousEndpointClientSessionResponse element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final MultiplexingSynchronousEndpointClientSessionResponse element) {
        element.clean();
    }

}
