package de.invesdwin.context.integration.channel.stream.server;

import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.session.MultiplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.session.SingleplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Closeables;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class StreamSynchronousEndpointServer extends ASynchronousEndpointServer {

    private final IStreamSynchronousEndpointServiceFactory serviceFactory;
    @GuardedBy("this")
    private final Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();

    public StreamSynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final IStreamSynchronousEndpointServiceFactory serviceFactory) {
        super(serverAcceptor);
        this.serviceFactory = serviceFactory;
    }

    @Override
    protected void onClose() {
        serviceId_service_sync.clear();
        serviceId_service_copy = new Int2ObjectOpenHashMap<>();
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
            return new SingleplexingStreamSynchronousEndpointServerSession(this, endpointSession);
        } else {
            //we want to be able to handle multiple
            return new MultiplexingStreamSynchronousEndpointServerSession(this, endpointSession);
        }
    }

    public IStreamSynchronousEndpointService getOrCreateService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        final IStreamSynchronousEndpointService service = getService(serviceId);
        if (service != null) {
            assertServiceSameTopic(service, topic);
            return service;
        } else {
            return registerService(serviceId, topic, parameters);
        }
    }

    private void assertServiceSameTopic(final IStreamSynchronousEndpointService service, final String topic) {
        if (service.getTopic().equals(topic)) {
            throw new IllegalStateException("serviceId [" + service.getServiceId() + "] topic mismatch: service.topic ["
                    + service.getTopic() + "] != topic [" + topic + "]");
        }
    }

    public IStreamSynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    private synchronized IStreamSynchronousEndpointService registerService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        final IStreamSynchronousEndpointService existing = serviceId_service_sync.get(serviceId);
        if (existing != null) {
            assertServiceSameTopic(existing, topic);
            return existing;
        }
        final IStreamSynchronousEndpointService service = serviceFactory.newService(serviceId, topic, parameters);
        Assertions.checkNull(serviceId_service_sync.put(service.getServiceId(), service));
        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
        return service;
    }

    public synchronized <T> boolean unregister(final int serviceId) {
        final IStreamSynchronousEndpointService removed = serviceId_service_sync.remove(serviceId);
        if (removed != null) {
            Closeables.closeQuietly(removed);
            //create a new copy of the map so that server thread does not require synchronization
            this.serviceId_service_copy = new Int2ObjectOpenHashMap<>(serviceId_service_sync);
            return true;
        } else {
            return false;
        }
    }
}
