package de.invesdwin.context.integration.channel.rpc.server.async;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.util.marshallers.serde.lookup.SerdeLookupConfig;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class AsynchronousEndpointServerHandlerFactory
        implements IAsynchronousHandlerFactory<IByteBufferProvider, IByteBufferProvider> {

    private final SerdeLookupConfig serdeLookupConfig;
    private final Int2ObjectMap<SynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<SynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();

    public AsynchronousEndpointServerHandlerFactory() {
        this(SerdeLookupConfig.DEFAULT);
    }

    public AsynchronousEndpointServerHandlerFactory(final SerdeLookupConfig serdeLookupConfig) {
        this.serdeLookupConfig = serdeLookupConfig;
    }

    public synchronized <T> void register(final Class<? super T> serviceInterface, final T serviceImplementation) {
        final SynchronousEndpointService service = SynchronousEndpointService.newInstance(serdeLookupConfig,
                serviceInterface, serviceImplementation);
        final SynchronousEndpointService existing = serviceId_service_sync.putIfAbsent(service.getServiceId(), service);
        if (existing != null) {
            throw new IllegalStateException("Already registered [" + service + "] as [" + existing + "]");
        }

        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
    }

    public synchronized <T> boolean unregister(final Class<? super T> serviceInterface) {
        final int serviceId = SynchronousEndpointService.newServiceId(serviceInterface);
        final SynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        return removed != null;
    }

    public SerdeLookupConfig getSerdeLookupConfig() {
        return serdeLookupConfig;
    }

    public SynchronousEndpointService getService(final int serviceId) {
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
        return new AsynchronousEndpointServerHandler(this);
    }

}
