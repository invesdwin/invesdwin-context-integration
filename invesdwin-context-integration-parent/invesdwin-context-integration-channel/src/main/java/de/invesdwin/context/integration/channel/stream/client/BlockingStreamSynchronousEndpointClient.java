package de.invesdwin.context.integration.channel.stream.client;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponse;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponsePool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.AbortRequestException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.LoggingDelegateUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.concurrent.future.ThrowableFuture;
import de.invesdwin.util.concurrent.pool.ICloseableObjectPool;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class BlockingStreamSynchronousEndpointClient implements IStreamSynchronousEndpointClient {

    private static final Log LOG = new Log(BlockingStreamSynchronousEndpointClient.class);

    private final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool;
    private ISynchronousEndpointClientSession session;
    @GuardedBy("this")
    private final Int2ObjectMap<TopicSubscription> serviceId_subscription_sync = new Int2ObjectOpenHashMap<>();
    private volatile Int2ObjectMap<TopicSubscription> serviceId_subscription_copy = new Int2ObjectOpenHashMap<>();
    private final IUnexpectedMessageListener pollUnexpectedMessageListener = new IUnexpectedMessageListener() {
        @Override
        public boolean onPushedWithoutRequest(final ISynchronousEndpointClientSession session, final int serviceId,
                final int methodId, final int streamSequence, final IByteBufferProvider message)
                throws AbortRequestException {
            final RuntimeException exception = LoggingDelegateUnexpectedMessageListener.maybeExtractException(methodId,
                    message);
            if (exception != null) {
                throw exception;
            }
            final TopicSubscription topicSubscription = serviceId_subscription_copy.get(serviceId);
            if (topicSubscription != null) {
                topicSubscription.onPush(streamSequence, message);
                throw AbortRequestException.getInstance("polling succeeded");
            } else {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(
                            "onPushedWithoutRequestAndWithoutSubscription sessionId=%s serviceId=%s methodId=%s streamSequence=%s message=%s",
                            session.getEndpointSession().getSessionId(), serviceId, methodId, streamSequence,
                            LoggingDelegateUnexpectedMessageListener.messageToString(methodId, message));
                }
            }
            return false;
        }

        @Override
        public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
                final int methodId, final int requestSequence, final IByteBufferProvider message) {
            final RuntimeException exception = LoggingDelegateUnexpectedMessageListener.maybeExtractException(methodId,
                    message);
            if (exception != null) {
                throw exception;
            }
            if (methodId == StreamServerMethodInfo.METHOD_ID_PUT) {
                //put responses are to be expected here
                return;
            }
            if (LOG.isWarnEnabled()) {
                LOG.warn("onUnexpectedResponse sessionId=%s serviceId=%s methodId=%s requestSequence=%s message=%s",
                        session.getEndpointSession().getSessionId(), serviceId, methodId, requestSequence,
                        LoggingDelegateUnexpectedMessageListener.messageToString(methodId, message));
            }
        }
    };
    private final IUnexpectedMessageListener requestUnexpectedMessageListener = new IUnexpectedMessageListener() {

        @Override
        public boolean onPushedWithoutRequest(final ISynchronousEndpointClientSession session, final int serviceId,
                final int methodId, final int streamSequence, final IByteBufferProvider message)
                throws AbortRequestException {
            final MultiplexingSynchronousEndpointClientSessionResponse response = MultiplexingSynchronousEndpointClientSessionResponsePool.INSTANCE
                    .borrowObject();
            response.init(serviceId, methodId, streamSequence, null, false, null, null, null);
            try {
                response.responseCompleted(message);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            return false;
        }

        @Override
        public void onUnexpectedResponse(final ISynchronousEndpointClientSession session, final int serviceId,
                final int methodId, final int requestSequence, final IByteBufferProvider message)
                throws AbortRequestException {
            pollUnexpectedMessageListener.onUnexpectedResponse(session, serviceId, methodId, requestSequence, message);
        }
    };
    private final ManyToOneConcurrentLinkedQueue<MultiplexingSynchronousEndpointClientSessionResponse> asyncStreamMessages = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public BlockingStreamSynchronousEndpointClient(
            final ICloseableObjectPool<ISynchronousEndpointClientSession> sessionPool) {
        this.sessionPool = sessionPool;
    }

    @Override
    public synchronized void open() throws IOException {
        if (!shouldOpen()) {
            return;
        }
        this.session = sessionPool.borrowObject();
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!shouldClose()) {
            return;
        }
        if (session != null) {
            sessionPool.returnObject(session);
            session = null;
        }
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    @Override
    public boolean poll(final Duration timeout) {
        try {
            final MultiplexingSynchronousEndpointClientSessionResponse polled = asyncStreamMessages.poll();
            if (polled != null) {
                try {
                    pollUnexpectedMessageListener.onPushedWithoutRequest(session, polled.getServiceId(),
                            polled.getMethodId(), polled.getRequestSequence(), polled);
                    return true;
                } finally {
                    polled.close();
                }
            }
            session.poll(timeout, pollUnexpectedMessageListener);
            return false;
        } catch (final TimeoutException e) {
            return false;
        } catch (final AbortRequestException e) {
            return true;
        }
    }

    @Override
    public Future<?> put(final int serviceId, final ICloseableByteBufferProvider message) {
        try {
            //fire-and-forget/non-blocking write in sequence, response will get logged/thrown in pollUnexpectedMessageListener
            session.request(serviceId, StreamServerMethodInfo.METHOD_ID_PUT, session.nextRequestSequence(), message,
                    true, getRequestTimeout(session), false, requestUnexpectedMessageListener);
            //TODO: return a real future that allows handling a potential exception response directly (e.g. server overload for back-off)
            /*
             * TODO: make sure that sequence is kept properly, even if we already have 5 messages in the queue and 1 in
             * the middle gets rejected and needs to be resent while the next ones were processed properly; server needs
             * to wait for the resend before writing the next sequenced messages so that we don't mix the order. Though
             * we could add this also as a reader/writer step that is useable with any transport transparently? Also
             * does the server write the message and then complain or does it actually reject the message fully?
             */
            /*
             * TODO: implement a truly non-blocking endpoint client session wrapper that directly returns futures for
             * requests and handles requests in a queue to keep the order. Also maybe pool futures and return a
             * ICloseableFuture?
             */
            return NullFuture.getInstance();
        } catch (final Throwable e) {
            return ThrowableFuture.of(e);
        }
    }

    protected Duration getRequestTimeout(final ISynchronousEndpointClientSession session) {
        return session.getDefaultRequestTimeout();
    }

    @Override
    public Future<?> create(final int serviceId, final String topicUri) {
        try (ICloseableByteBuffer message = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int length = message.putStringUtf8(0, topicUri);
            session.request(serviceId, StreamServerMethodInfo.METHOD_ID_CREATE, session.nextRequestSequence(),
                    message.sliceTo(length), false, getRequestTimeout(session), true, requestUnexpectedMessageListener);
            return NullFuture.getInstance();
        } catch (final Throwable e) {
            return ThrowableFuture.of(e);
        }
    }

    @Override
    public Future<?> subscribe(final int serviceId, final String topicUri,
            final IStreamSynchronousEndpointClientSubscription subscription) {
        registerSubscription(serviceId, topicUri, subscription);
        try (ICloseableByteBuffer message = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int length = message.putStringUtf8(0, topicUri);
            session.request(serviceId, StreamServerMethodInfo.METHOD_ID_SUBSCRIBE, session.nextRequestSequence(),
                    message.sliceTo(length), false, getRequestTimeout(session), true, requestUnexpectedMessageListener);
            return NullFuture.getInstance();
        } catch (final Throwable e) {
            return ThrowableFuture.of(e);
        }
    }

    @Override
    public Future<?> unsubscribe(final int serviceId, final String topicUri) {
        unregisterSubscription(serviceId);
        try (ICloseableByteBuffer message = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int length = message.putStringUtf8(0, topicUri);
            session.request(serviceId, StreamServerMethodInfo.METHOD_ID_UNSUBSCRIBE, session.nextRequestSequence(),
                    message.sliceTo(length), false, getRequestTimeout(session), true, requestUnexpectedMessageListener);
            return NullFuture.getInstance();
        } catch (final Throwable e) {
            return ThrowableFuture.of(e);
        }
    }

    @Override
    public Future<?> delete(final int serviceId, final String topicUri) {
        try (ICloseableByteBuffer message = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int length = message.putStringUtf8(0, topicUri);
            session.request(serviceId, StreamServerMethodInfo.METHOD_ID_DELETE, session.nextRequestSequence(),
                    message.sliceTo(length), false, getRequestTimeout(session), true, requestUnexpectedMessageListener);
            return NullFuture.getInstance();
        } catch (final Throwable e) {
            return ThrowableFuture.of(e);
        }
    }

    private synchronized void registerSubscription(final int serviceId, final String topicUri,
            final IStreamSynchronousEndpointClientSubscription subscription) {
        Assertions.checkNull(serviceId_subscription_sync.putIfAbsent(serviceId,
                new TopicSubscription(serviceId, topicUri, subscription)));
        //create a new copy of the map so that server thread does not require synchronization
        this.serviceId_subscription_copy = new Int2ObjectOpenHashMap<>(serviceId_subscription_sync);
    }

    private synchronized <T> boolean unregisterSubscription(final int serviceId) {
        final TopicSubscription removed = serviceId_subscription_sync.remove(serviceId);
        if (removed != null) {
            Closeables.closeQuietly(removed);
            //create a new copy of the map so that server thread does not require synchronization
            this.serviceId_subscription_copy = new Int2ObjectOpenHashMap<>(serviceId_subscription_sync);
            return true;
        } else {
            return false;
        }
    }

    private static final class TopicSubscription {

        private final int serviceId;
        private final String topic;
        private final IStreamSynchronousEndpointClientSubscription subscription;

        private TopicSubscription(final int serviceId, final String topicUri,
                final IStreamSynchronousEndpointClientSubscription subscription) {
            this.serviceId = serviceId;
            this.topic = URIs.getBasis(topicUri);
            this.subscription = subscription;
        }

        public void onPush(final int streamSequence, final IByteBufferProvider message) {
            subscription.onPush(serviceId, topic, message);
        }

    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(session).toString();
    }

}
