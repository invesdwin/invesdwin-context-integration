package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.Closeable;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator.INode;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A stream session manager that handles pushing messages for subscriptions and requests from clients.
 */
@ThreadSafe
public class DefaultStreamSynchronousEndpointServerSessionManager
        implements IStreamSynchronousEndpointServerSessionManager {

    private final IStreamSynchronousEndpointServerSession session;
    private final StreamSynchronousEndpointServer server;
    @GuardedBy("self")
    private final Int2ObjectMap<Subscription> serviceId_subscription = new Int2ObjectOpenHashMap<>();
    private final NodeBufferingIterator<Subscription> notifiedSubscriptions = new NodeBufferingIterator<>();

    public DefaultStreamSynchronousEndpointServerSessionManager(final IStreamSynchronousEndpointServerSession session) {
        this.session = session;
        this.server = session.getParent();
    }

    @Override
    public boolean handle() {
        boolean finished = true;
        try {
            while (true) {
                final Subscription notifiedSubscription;
                synchronized (notifiedSubscriptions) {
                    notifiedSubscription = notifiedSubscriptions.getHead();
                }
                if (notifiedSubscription.handle()) {
                    synchronized (notifiedSubscriptions) {
                        Assertions.checkSame(notifiedSubscriptions.next(), notifiedSubscription);
                    }
                } else {
                    finished = false;
                }
            }
        } catch (final NoSuchElementException e) {
            //end reached
        }
        return finished;
    }

    @Override
    public IStreamSynchronousEndpointServerSession getSession() {
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
    public boolean isFuture(final StreamServerMethodInfo streamServerMethodInfo) {
        return false;
    }

    @Override
    public Object put(final IStreamSynchronousEndpointService service, final IByteBufferProvider message) {
        return null;
    }

    @Override
    public Object subscribe(final IStreamSynchronousEndpointService service, final Map<String, String> parameters) {
        final int serviceId = service.getServiceId();
        synchronized (serviceId_subscription) {
            final Subscription existing = serviceId_subscription.get(serviceId);
            if (existing != null) {
                throw new IllegalStateException(
                        "already subscribed to serviceId [" + serviceId + "] with topic: " + service.getTopic());
            }
            serviceId_subscription.put(serviceId, new Subscription(service, notifiedSubscriptions, parameters));
        }
        return null;
    }

    @Override
    public synchronized Object unsubscribe(final IStreamSynchronousEndpointService service,
            final Map<String, String> parameters) {
        final int serviceId = service.getServiceId();
        final Subscription removed;
        synchronized (serviceId_subscription) {
            removed = serviceId_subscription.remove(serviceId);
            if (removed == null) {
                throw new IllegalStateException(
                        "not subscribed to serviceId [" + serviceId + "] with topic: " + service.getTopic());
            }
        }
        removed.close();
        return null;
    }

    @Override
    public Object delete(final IStreamSynchronousEndpointService service, final Map<String, String> parameters) {
        service.delete();
        return null;
    }

    private static final class Subscription implements Runnable, Closeable, INode<Subscription> {
        private final IStreamSynchronousEndpointService service;
        @GuardedBy("self")
        private final NodeBufferingIterator<Subscription> notifiedSubscriptions;
        private final ISynchronousReader<IByteBufferProvider> reader;
        private Subscription next;
        private Subscription prev;
        private volatile boolean notified;

        private Subscription(final IStreamSynchronousEndpointService service,
                final NodeBufferingIterator<Subscription> notifiedSubscriptions, final Map<String, String> parameters) {
            this.notifiedSubscriptions = notifiedSubscriptions;
            this.service = service;
            this.reader = service.subscribe(this, parameters);
        }

        public boolean handle() {
            notified = false;
            return true;
        }

        @Override
        public void close() {
            if (!service.unsubscribe(this)) {
                throw new IllegalStateException(
                        "subscription to service [" + service.getServiceId() + "] already unsubscribed");
            }
        }

        @Override
        public void run() {
            if (notified) {
                return;
            }
            notified = true;
            synchronized (notifiedSubscriptions) {
                notifiedSubscriptions.add(this);
            }
        }

        @Override
        public Subscription getNext() {
            return next;
        }

        @Override
        public void setNext(final Subscription next) {
            this.next = next;
        }

        @Override
        public Subscription getPrev() {
            return prev;
        }

        @Override
        public void setPrev(final Subscription prev) {
            this.prev = prev;
        }
    }

}
