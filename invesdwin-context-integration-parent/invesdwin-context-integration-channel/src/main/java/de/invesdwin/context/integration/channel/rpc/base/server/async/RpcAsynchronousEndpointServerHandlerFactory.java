package de.invesdwin.context.integration.channel.rpc.base.server.async;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.base.server.RpcSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.AsyncPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.IPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class RpcAsynchronousEndpointServerHandlerFactory implements IAsynchronousEndpointServerHandlerFactory {

    public static final int DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL = RpcSynchronousEndpointServer.DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    public static final int DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION = Integers
            .abs(RpcSynchronousEndpointServer.DEFAULT_INITIAL_MAX_PENDING_WORK_COUNT_PER_SESSION);
    public static final WrappedExecutorService DEFAULT_WORK_EXECUTOR = RpcSynchronousEndpointServer.DEFAULT_WORK_EXECUTOR;
    private final SerdeLookupConfig serdeLookupConfig;
    private final Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    private final WrappedExecutorService workExecutor;
    private final int maxPendingWorkCountOverall;
    private final int maxPendingWorkCountPerSession;
    private IPollingQueueProvider pollingQueueProvider = AsyncPollingQueueProvider.INSTANCE;

    public RpcAsynchronousEndpointServerHandlerFactory() {
        this(SerdeLookupConfig.DEFAULT);
    }

    public RpcAsynchronousEndpointServerHandlerFactory(final SerdeLookupConfig serdeLookupConfig) {
        this.serdeLookupConfig = serdeLookupConfig;
        this.workExecutor = newWorkExecutor();
        this.maxPendingWorkCountOverall = newMaxPendingWorkCountOverall();
        if (maxPendingWorkCountOverall < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountOverall should not be negative: " + maxPendingWorkCountOverall);
        }
        this.maxPendingWorkCountPerSession = newMaxPendingWorkCountPerSession();
        if (maxPendingWorkCountPerSession < 0) {
            throw new IllegalArgumentException(
                    "maxPendingWorkCountPerSession should not be negative: " + maxPendingWorkCountPerSession);
        }
    }

    @Override
    public void setPollingQueueProvider(final IPollingQueueProvider pollingQueueProvider) {
        this.pollingQueueProvider = pollingQueueProvider;
    }

    @Override
    public IPollingQueueProvider getPollingQueueProvider() {
        return pollingQueueProvider;
    }

    /**
     * Further requests will be rejected if the workExecutor has more than that amount of requests pending. Only applies
     * when workExecutor is not null.
     * 
     * return 0 here for unlimited pending work count overall.
     */
    protected int newMaxPendingWorkCountOverall() {
        return DEFAULT_MAX_PENDING_WORK_COUNT_OVERALL;
    }

    /**
     * Return 0 here for unlimited pending work count per session, will be limited by overall pending work count only
     * which means that one rogue client can take all resources for himself (not advisable unless clients can be
     * trusted).
     * 
     * Return a positive value here to limit the pending requests
     */
    protected int newMaxPendingWorkCountPerSession() {
        return DEFAULT_MAX_PENDING_WORK_COUNT_PER_SESSION;
    }

    /**
     * Can return null here to not use an executor for work handling, instead the IO thread will handle it directly.
     * This is preferred when request execution is very fast does not involve complex calculations.
     * 
     * Use LIMITED_WORK_EXECUTOR when cpu intensive or long running tasks are performed that should not block the IO of
     * the request thread. In Java 21 it might be a good choice to use a virtual thread executor here so that IO and
     * sleeps don't block work on other threads (maybe use a higher threshold than 10k for pending tasks then?).
     * Otherwise a thread pool with a higher size than cpu count should be used that allows IO/sleep between response
     * work processing.
     */
    protected WrappedExecutorService newWorkExecutor() {
        return DEFAULT_WORK_EXECUTOR;
    }

    public final WrappedExecutorService getWorkExecutor() {
        return workExecutor;
    }

    public int getMaxPendingWorkCountOverall() {
        return maxPendingWorkCountOverall;
    }

    public int getMaxPendingWorkCountPerSession() {
        return maxPendingWorkCountPerSession;
    }

    public synchronized <T> void register(final Class<? super T> serviceInterface, final T serviceImplementation) {
        final RpcSynchronousEndpointService service = RpcSynchronousEndpointService.newInstance(serdeLookupConfig,
                serviceInterface, serviceImplementation);
        final RpcSynchronousEndpointService existing = serviceId_service_sync.putIfAbsent(service.getServiceId(), service);
        if (existing != null) {
            throw new IllegalStateException("Already registered [" + service + "] as [" + existing + "]");
        }

        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
    }

    public synchronized <T> boolean unregister(final Class<? super T> serviceInterface) {
        final int serviceId = RpcSynchronousEndpointService.newServiceId(serviceInterface);
        final RpcSynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        return removed != null;
    }

    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    public RpcSynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    @Override
    public void open() throws IOException {}

    @Override
    public synchronized void close() throws IOException {
        serviceId_service_sync.clear();
        serviceId_service_copy = null;
    }

    @Override
    public IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> newHandler() {
        return new RpcAsynchronousEndpointServerHandler(this);
    }

}
