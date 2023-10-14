package de.invesdwin.context.integration.channel.rpc.server.blocking.context;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSessionFactory;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.blocking.ABlockingEndpointServer;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public final class BlockingEndpointServiceHandlerContextPool
        extends AAgronaObjectPool<BlockingEndpointServiceHandlerContext> {

    private static final String KEY_MAX_POOL_SIZE = "MAX_POOL_SIZE";
    private final ABlockingEndpointServer parent;
    private final ServerSideBlockingEndpoint endpoint;
    private final ISynchronousEndpointSession endpointSession;

    public BlockingEndpointServiceHandlerContextPool(final ABlockingEndpointServer parent) {
        super(newMaxPoolSize());
        this.parent = parent;
        this.endpoint = new ServerSideBlockingEndpoint();
        final ISynchronousEndpointSessionFactory endpointSessionFactory = parent.getSessionFactoryTransformer()
                .transform(new ISynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider>() {
                    @Override
                    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> newEndpoint() {
                        return endpoint;
                    }
                });
        this.endpointSession = endpointSessionFactory.newSession();
    }

    private static int newMaxPoolSize() {
        return new SystemProperties(BlockingEndpointServiceHandlerContextPool.class).getIntegerOptional(
                KEY_MAX_POOL_SIZE, SynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL);
    }

    @Override
    protected BlockingEndpointServiceHandlerContext newObject() {
        final IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler = parent.getHandlerFactory()
                .newHandler();
        return new BlockingEndpointServiceHandlerContext(this, endpoint, endpointSession, handler);
    }

    @Override
    protected boolean passivateObject(final BlockingEndpointServiceHandlerContext element) {
        element.clean();
        return true;
    }

    @Override
    public void invalidateObject(final BlockingEndpointServiceHandlerContext element) {
        element.clean();
    }

}
