package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.system.properties.DisabledProperties;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A stream session manager that handles pushing messages for subscriptions and creating responses for requests from
 * clients.
 */
@ThreadSafe
public class DefaultStreamSessionManager implements IStreamSessionManager {

    private final IStreamSynchronousEndpointSession session;
    private final IStreamSynchronousEndpointServer server;
    @GuardedBy("self")
    private final Int2ObjectMap<DefaultStreamSessionManagerSubscription> serviceId_subscription = new Int2ObjectOpenHashMap<>();
    @GuardedBy("self")
    private final NodeBufferingIterator<DefaultStreamSessionManagerSubscription> notifiedSubscriptions = new NodeBufferingIterator<>();

    public DefaultStreamSessionManager(final IStreamSynchronousEndpointSession session) {
        this.session = session;
        this.server = session.getServer();
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
                if (notifiedSubscription == null) {
                    break;
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
            //end reached for subscriptions or session writer is too busy, thus continue with the next session
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
            final IProperties parameters) throws IOException {
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
            /*
             * Here we could theoretically offload the message into a local queue (NodeBufferingIterator) with a message
             * copy so it gets retried to be written in the handle call of the session. Though this might be inefficient
             * because multiple IO/worker threads could be trying to push messages simultaneously, which would cause
             * thread synchronization overhead. It is better to keep track of an acceptable backlog/queue in the service
             * directly instead of having a backlog on each session individually (which could exhaust memory
             * uncontrollably). So instead we just throw an exception if service is overloaded to tell the client to
             * retry later.
             */
            throw new RetryLaterRuntimeException("Unable to put message into serviceId [" + service.getServiceId()
                    + "] for topic [" + service.getTopic()
                    + "]. Please back off a bit with the writing since storage seems to busy.");
        }
        return null;
    }

    @Override
    public boolean isAlwaysFutureSubscribe() {
        return false;
    }

    @Override
    public Object subscribe(final IStreamSynchronousEndpointService service, final IProperties parameters)
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
            final IProperties parameters) throws Exception {
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
    public Object delete(final IStreamSynchronousEndpointService service, final IProperties parameters)
            throws Exception {
        if (!service.delete(parameters)) {
            throw new IllegalStateException(
                    "can not delete serviceId [" + service.getServiceId() + "] for topic: " + service.getTopic());
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        final List<DefaultStreamSessionManagerSubscription> subscriptionsCopy;
        synchronized (serviceId_subscription) {
            subscriptionsCopy = new ArrayList<>(serviceId_subscription.values());
        }
        for (final DefaultStreamSessionManagerSubscription subscription : subscriptionsCopy) {
            subscription.unsubscribe(DisabledProperties.INSTANCE);
        }
        synchronized (serviceId_subscription) {
            serviceId_subscription.clear();
        }
        synchronized (notifiedSubscriptions) {
            notifiedSubscriptions.clear();
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(session).toString();
    }

}
