package de.invesdwin.context.integration.channel.rpc.base.server;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.rpc.base.server.session.MultiplexingRpcSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.rpc.base.server.session.SingleplexingRpcSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class RpcSynchronousEndpointServer extends ASynchronousEndpointServer {

    private final SerdeLookupConfig serdeLookupConfig;
    @GuardedBy("this")
    private final Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();

    public RpcSynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor) {
        super(serverAcceptor);
        this.serdeLookupConfig = newSerdeLookupConfig();
    }

    protected SerdeLookupConfig newSerdeLookupConfig() {
        return SerdeLookupConfig.DEFAULT;
    }

    @Override
    protected ISynchronousEndpointServerSession newServerSession(final ISynchronousEndpointSession endpointSession) {
        if (getWorkExecutor() == null) {
            /*
             * Singlexplexing can not handle more than 1 request at a time, so this is the most efficient. Though could
             * also be used with workExecutor to limit concurrent requests different to IO threads. But IO threads are
             * normally good enough when requests are not expensive. Though if there is a mix between expensive and fast
             * requests, then a work executor with Singleplexing might be preferable. In all other cases I guess
             * multiplexing should be favored.
             */
            return new SingleplexingRpcSynchronousEndpointServerSession(this, endpointSession);
        } else {
            //we want to be able to handle multiple
            return new MultiplexingRpcSynchronousEndpointServerSession(this, endpointSession);
        }
    }

    public synchronized <T> void register(final Class<? super T> serviceInterface, final T serviceImplementation) {
        final RpcSynchronousEndpointService service = RpcSynchronousEndpointService.newInstance(serdeLookupConfig,
                serviceInterface, serviceImplementation);
        final RpcSynchronousEndpointService existing = serviceId_service_sync.putIfAbsent(service.getServiceId(),
                service);
        if (existing != null) {
            throw new IllegalStateException("Already registered [" + service + "] as [" + existing + "]");
        }

        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
    }

    public synchronized <T> boolean unregister(final Class<? super T> serviceInterface) {
        final int serviceId = RpcSynchronousEndpointService.newServiceId(serviceInterface);
        final RpcSynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        if (removed != null) {
            //create a new copy of the map so that server thread does not require synchronization
            this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void onClose() {
        serviceId_service_sync.clear();
        serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    }

    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    public RpcSynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

}
