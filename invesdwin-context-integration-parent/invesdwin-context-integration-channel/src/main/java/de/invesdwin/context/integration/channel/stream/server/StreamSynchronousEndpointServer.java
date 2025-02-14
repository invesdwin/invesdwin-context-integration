package de.invesdwin.context.integration.channel.stream.server;

import java.util.Map;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.stream.server.session.MultiplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.session.SingleplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.session.manager.DefaultStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Closeables;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class StreamSynchronousEndpointServer extends ASynchronousEndpointServer {

    public static final int DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SESSION = 100;
    public static final int DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SUBSCRIPTION = 50;

    private final IStreamSynchronousEndpointServiceFactory serviceFactory;
    @GuardedBy("this")
    private final Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<IStreamSynchronousEndpointService> serviceId_service_copy = new Int2ObjectOpenHashMap<>();
    private final int maxSuccessivePushCountPerSession;
    private final int maxSuccessivePushCountPerSubscription;

    public StreamSynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor,
            final IStreamSynchronousEndpointServiceFactory serviceFactory) {
        super(serverAcceptor);
        this.serviceFactory = serviceFactory;
        this.maxSuccessivePushCountPerSession = newMaxSuccessivePushCountPerSession();
        this.maxSuccessivePushCountPerSubscription = newMaxSuccessivePushCountPerSubscription();
    }

    /**
     * This defines how many consecutive topic messages can be pushed before checking for the next request and giving
     * that request priority in handling before pushing again
     */
    protected int newMaxSuccessivePushCountPerSession() {
        return DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SESSION;
    }

    /**
     * This defines how many consecutive topic messages can be pushed for an individual subscription before checking the
     * next subscription. This allows to give other topics the chance to send messages without being blocking by a
     * particularly busy topic.
     */
    protected int newMaxSuccessivePushCountPerSubscription() {
        return DEFAULT_MAX_SUCCESSIVE_PUSH_COUNT_PER_SUBSCRIPTION;
    }

    public int getMaxSuccessivePushCountPerSession() {
        return maxSuccessivePushCountPerSession;
    }

    public int getMaxSuccessivePushCountPerSubscription() {
        return maxSuccessivePushCountPerSubscription;
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
            //we want to be able to handle multiple requests concurrently
            return new MultiplexingStreamSynchronousEndpointServerSession(this, endpointSession);
        }
    }

    public IStreamSynchronousEndpointService getOrCreateService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        final IStreamSynchronousEndpointService service = getService(serviceId);
        if (service != null) {
            StreamServerMethodInfo.assertServiceTopic(service, topic);
            return service;
        } else {
            return registerService(serviceId, topic, parameters);
        }
    }

    public IStreamSynchronousEndpointService getService(final int serviceId) {
        return serviceId_service_copy.get(serviceId);
    }

    private synchronized IStreamSynchronousEndpointService registerService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        final IStreamSynchronousEndpointService existing = serviceId_service_sync.get(serviceId);
        if (existing != null) {
            StreamServerMethodInfo.assertServiceTopic(existing, topic);
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

    public IStreamSessionManager newManager(final IStreamSynchronousEndpointSession session) {
        return new DefaultStreamSessionManager(session);
    }
}
