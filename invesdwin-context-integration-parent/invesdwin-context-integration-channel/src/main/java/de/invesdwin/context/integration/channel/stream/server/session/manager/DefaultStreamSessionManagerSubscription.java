package de.invesdwin.context.integration.channel.stream.server.session.manager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceListener;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator.INode;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class DefaultStreamSessionManagerSubscription
        implements IStreamSynchronousEndpointServiceListener, INode<DefaultStreamSessionManagerSubscription> {
    private final IStreamSessionManager manager;
    private final IStreamSynchronousEndpointSession session;
    private final IStreamSynchronousEndpointServer server;
    private final IStreamSynchronousEndpointService service;
    @GuardedBy("self")
    private final NodeBufferingIterator<DefaultStreamSessionManagerSubscription> notifiedSubscriptions;
    private final ReadFinishedDelegateSynchronousReader<IByteBufferProvider> reader;
    private DefaultStreamSessionManagerSubscription next;
    private DefaultStreamSessionManagerSubscription prev;
    private final AtomicBoolean notified = new AtomicBoolean();
    private int burstMessages = 0;

    public DefaultStreamSessionManagerSubscription(final IStreamSessionManager manager,
            final IStreamSynchronousEndpointService service,
            final NodeBufferingIterator<DefaultStreamSessionManagerSubscription> notifiedSubscriptions,
            final IProperties parameters) throws Exception {
        this.manager = manager;
        this.session = manager.getSession();
        this.server = session.getServer();
        this.service = service;
        this.notifiedSubscriptions = notifiedSubscriptions;
        final ISynchronousReader<IByteBufferProvider> subscription = service.subscribe(this, parameters);
        Assertions.checkNotNull(subscription);
        final ReadFinishedDelegateSynchronousReader<IByteBufferProvider> reader = new ReadFinishedDelegateSynchronousReader<IByteBufferProvider>(
                subscription);
        reader.open();
        this.reader = reader;
    }

    public boolean handle() throws IOException {
        if (reader == null) {
            return false;
        }
        if (!reader.isReadFinished()) {
            //last message from this reader is still being written to the client in the sessoion, wait for that to be finished
            throw FastNoSuchElementException.getInstance("reader.isReadFinished is false");
        }
        boolean handledOverall = false;
        while (reader.hasNext()) {
            final boolean pushFinished = session.pushSubscriptionMessage(service, reader);
            if (!pushFinished) {
                //response channel is busy, we have to wait a bit before we can attempt to send additional messages
                return false;
            } else {
                if (burstMessages == 0) {
                    //give pushing messages on this subscription priority
                    burstMessages = server.getMaxSuccessivePushCountPerSubscription();
                } else {
                    //decrease priority for pushing messages on this subcription
                    burstMessages--;
                    if (burstMessages == 0) {
                        //give another subscription the chance to write a few messages as well
                        synchronized (notifiedSubscriptions) {
                            if (notifiedSubscriptions.size() > 1) {
                                Assertions.checkTrue(notifiedSubscriptions.remove(this));
                                Assertions.checkTrue(notifiedSubscriptions.add(this));
                            }
                        }
                        return false;
                    }
                }
                handledOverall = true;
            }
        }
        //we can check other subscriptions again now
        burstMessages = 0;
        if (!handledOverall) {
            if (notified.compareAndSet(true, false)) {
                synchronized (notifiedSubscriptions) {
                    Assertions.checkTrue(notifiedSubscriptions.remove(this));
                }
            }
        }
        return handledOverall;
    }

    public void unsubscribe(final IProperties parameters) throws IOException {
        if (!service.unsubscribe(this, parameters)) {
            throw new IllegalStateException(
                    "subscription to service [" + service.getServiceId() + "] already unsubscribed");
        }
        if (notified.compareAndSet(true, false)) {
            synchronized (notifiedSubscriptions) {
                Assertions.checkTrue(notifiedSubscriptions.remove(this));
            }
        }
        reader.close();
    }

    @Override
    public void onPut() throws Exception {
        if (notified.compareAndSet(false, true)) {
            synchronized (notifiedSubscriptions) {
                Assertions.checkTrue(notifiedSubscriptions.add(this));
            }
        }
    }

    @Override
    public void onDelete(final IProperties parameters) throws Exception {
        manager.unsubscribe(service, parameters);
    }

    @Override
    public DefaultStreamSessionManagerSubscription getNext() {
        return next;
    }

    @Override
    public void setNext(final DefaultStreamSessionManagerSubscription next) {
        this.next = next;
    }

    @Override
    public DefaultStreamSessionManagerSubscription getPrev() {
        return prev;
    }

    @Override
    public void setPrev(final DefaultStreamSessionManagerSubscription prev) {
        this.prev = prev;
    }
}