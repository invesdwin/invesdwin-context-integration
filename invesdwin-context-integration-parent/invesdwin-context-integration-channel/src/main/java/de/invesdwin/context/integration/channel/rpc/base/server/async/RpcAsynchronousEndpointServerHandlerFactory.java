package de.invesdwin.context.integration.channel.rpc.base.server.async;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class RpcAsynchronousEndpointServerHandlerFactory extends AAsynchronousEndpointServerHandlerFactory {

    private final SerdeLookupConfig serdeLookupConfig;
    private final Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<RpcSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();

    public RpcAsynchronousEndpointServerHandlerFactory() {
        this(SerdeLookupConfig.DEFAULT);
    }

    public RpcAsynchronousEndpointServerHandlerFactory(final SerdeLookupConfig serdeLookupConfig) {
        this.serdeLookupConfig = serdeLookupConfig;
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
        return removed != null;
    }

    public final SerdeLookupConfig getSerdeLookupConfig() {
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
