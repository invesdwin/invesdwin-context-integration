package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A stream session manager that handles pushing messages for subscriptions and requests from clients.
 */
@ThreadSafe
public class DefaultStreamSessionManager implements IStreamSessionManager {

    private final IStreamSynchronousEndpointSession session;
    private final StreamSynchronousEndpointServer server;
    @GuardedBy("self")
    private final Int2ObjectMap<DefaultStreamSessionManagerSubscription> serviceId_subscription = new Int2ObjectOpenHashMap<>();
    private final NodeBufferingIterator<DefaultStreamSessionManagerSubscription> notifiedSubscriptions = new NodeBufferingIterator<>();

    public DefaultStreamSessionManager(final IStreamSynchronousEndpointSession session) {
        this.session = session;
        this.server = session.getParent();
    }

    @Override
    public boolean handle() throws IOException {
        boolean handledOverall = false;
        try {
            while (true) {
                final DefaultStreamSessionManagerSubscription notifiedSubscription;
                synchronized (notifiedSubscriptions) {
                    notifiedSubscription = notifiedSubscriptions.getHead();
                }
                final boolean handledNow = notifiedSubscription.handle();
                if (handledNow) {
                    handledOverall = true;
                } else if (handledOverall) {
                    //response channel is busy writing messages, makes no sense to write additional messages while it is busy
                    break;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return handledOverall;
    }

    @Override
    public IStreamSynchronousEndpointSession getSession() {
        return session;
    }

    @Override
    public IStreamSynchronousEndpointService getService(final int serviceId) {
        return server.getService(serviceId);
    }

    @Override
    public IStreamSynchronousEndpointService getOrCreateService(final int serviceId, final String topic,
            final Map<String, String> parameters) {
        return server.getOrCreateService(serviceId, topic, parameters);
    }

    @Override
    public boolean isAlwaysFuturePut() {
        return false;
    }

    @Override
    public Object put(final IStreamSynchronousEndpointService service, final IByteBufferProvider message)
            throws Exception {
        if (!service.put(message)) {
            throw new IllegalStateException("unable to put message into serviceId [" + service.getServiceId()
                    + "] for topic: " + service.getTopic());
        }
        return null;
    }

    @Override
    public boolean isAlwaysFutureSubscribe() {
        return false;
    }

    @Override
    public Object subscribe(final IStreamSynchronousEndpointService service, final Map<String, String> parameters)
            throws Exception {
        final int serviceId = service.getServiceId();
        synchronized (serviceId_subscription) {
            final DefaultStreamSessionManagerSubscription existing = serviceId_subscription.get(serviceId);
            if (existing != null) {
                throw new IllegalStateException(
                        "already subscribed to serviceId [" + serviceId + "] with topic: " + service.getTopic());
            }
            Assertions.checkNull(serviceId_subscription.put(serviceId,
                    new DefaultStreamSessionManagerSubscription(this, service, notifiedSubscriptions, parameters)));
        }
        return null;
    }

    @Override
    public boolean isAlwaysFutureUnsubscribe() {
        return false;
    }

    @Override
    public synchronized Object unsubscribe(final IStreamSynchronousEndpointService service,
            final Map<String, String> parameters) throws Exception {
        final int serviceId = service.getServiceId();
        final DefaultStreamSessionManagerSubscription removed;
        synchronized (serviceId_subscription) {
            removed = serviceId_subscription.remove(serviceId);
            if (removed == null) {
                throw new IllegalStateException(
                        "not subscribed to serviceId [" + serviceId + "] with topic: " + service.getTopic());
            }
        }
        removed.unsubscribe(parameters);
        return null;
    }

    @Override
    public boolean isAlwaysFutureDelete() {
        return false;
    }

    @Override
    public Object delete(final IStreamSynchronousEndpointService service, final Map<String, String> parameters)
            throws Exception {
        if (!service.delete(parameters)) {
            throw new IllegalStateException(
                    "can not delete serviceId [" + service.getServiceId() + "] for topic: " + service.getTopic());
        }
        return null;
    }

}
