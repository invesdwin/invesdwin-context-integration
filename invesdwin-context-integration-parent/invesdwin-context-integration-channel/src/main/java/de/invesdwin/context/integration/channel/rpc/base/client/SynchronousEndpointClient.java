package de.invesdwin.context.integration.channel.rpc.base.client;

import java.lang.reflect.Proxy;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.handler.RpcSynchronousEndpointClientHandler;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.server.RpcSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;

@ThreadSafe
public class SynchronousEndpointClient<T> implements ISynchronousEndpointClient<T> {

    public static final int DEFAULT_MAX_PENDING_WORK_COUNT = RpcAsynchronousEndpointServerHandlerFactory.DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION;

    public static final WrappedExecutorService DEFAULT_FUTURE_EXECUTOR = Executors
            .newFixedThreadPool(RpcSynchronousEndpointServer.class.getSimpleName() + "_FUTURE",
                    DEFAULT_MAX_PENDING_WORK_COUNT)
            .setDynamicThreadName(false);

    private final Class<T> serviceInterface;
    private final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool;
    private final SerdeLookupConfig serdeLookupConfig;
    private final WrappedExecutorService futureExecutor;
    private final RpcSynchronousEndpointClientHandler handler;
    private final T service;

    @SuppressWarnings("unchecked")
    public SynchronousEndpointClient(final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool,
            final Class<T> serviceInterface) {
        this.serviceInterface = serviceInterface;
        this.sessionPool = sessionPool;
        this.serdeLookupConfig = newSerdeLookupConfig();
        this.futureExecutor = newFutureExecutor();
        this.handler = new RpcSynchronousEndpointClientHandler(this);
        this.service = (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(), new Class[] { serviceInterface },
                handler);
    }

    protected SerdeLookupConfig newSerdeLookupConfig() {
        return SerdeLookupConfig.DEFAULT;
    }

    /**
     * Return null here to disable async execution of futures. The calling thread will block for as long as the future
     * is not finished and return an ImmutableFuture once finished.
     */
    protected WrappedExecutorService newFutureExecutor() {
        return DEFAULT_FUTURE_EXECUTOR;
    }

    @Override
    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    @Override
    public WrappedExecutorService getFutureExecutor() {
        return futureExecutor;
    }

    @Override
    public ICloseableObjectPool<ISynchronousEndpointClientSession> getSessionPool() {
        return sessionPool;
    }

    @Override
    public int getServiceId() {
        return handler.getServiceId();
    }

    @Override
    public Class<T> getServiceInterface() {
        return serviceInterface;
    }

    @Override
    public T getService() {
        return service;
    }

    @Override
    public void close() {
        sessionPool.close();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serviceId", getServiceId())
                .add("serviceInterface", serviceInterface.getName())
                .toString();
    }

}
