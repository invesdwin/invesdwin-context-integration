package de.invesdwin.context.integration.channel.rpc.base.client.session.single;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.MultiplexingSynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.AbortRequestException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.FastTimeoutException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public class SingleplexingSynchronousEndpointClientSession implements ISynchronousEndpointClientSession {

    @GuardedBy("lock")
    private ISynchronousEndpointSession endpointSession;
    @GuardedBy("lock")
    private SynchronousWriterSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> requestWriterSpinWait;
    @GuardedBy("lock")
    private final MutableServiceSynchronousCommand<IByteBufferProvider> requestHolder = new MutableServiceSynchronousCommand<IByteBufferProvider>();
    @GuardedBy("lock")
    private SynchronousReaderSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> responseReaderSpinWait;
    @GuardedBy("lock")
    private long lastHeartbeatNanos = System.nanoTime();
    private final AtomicInteger requestSequenceCounter = new AtomicInteger();
    private final AtomicInteger streamSequenceCounter = new AtomicInteger();
    private final ILock lock;
    @GuardedBy("lock")
    private final ScheduledFuture<?> heartbeatFuture;
    private volatile boolean closed;

    private final SingleplexingSynchronousEndpointClientSessionResponse response;

    public SingleplexingSynchronousEndpointClientSession(final IObjectPool<ISynchronousEndpointClientSession> pool,
            final ISynchronousEndpointSession endpointSession) {
        this.endpointSession = endpointSession;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(SingleplexingSynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriterSpinWait = new SynchronousWriterSpinWait<>(
                endpointSession.newCommandRequestWriter(ByteBufferProviderSerde.GET));
        this.responseReaderSpinWait = new SynchronousReaderSpinWait<>(
                endpointSession.newCommandResponseReader(ByteBufferProviderSerde.GET));
        try {
            requestWriterSpinWait.getWriter().open();
            responseReaderSpinWait.getReader().open();
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
        this.heartbeatFuture = HEARTBEAT_EXECUTOR.scheduleWithFixedDelay(this::maybeSendHeartbeat,
                endpointSession.getHeartbeatInterval().longValue(), endpointSession.getHeartbeatInterval().longValue(),
                endpointSession.getHeartbeatInterval().getTimeUnit().timeUnitValue());
        this.response = new SingleplexingSynchronousEndpointClientSessionResponse(pool, this, lock,
                responseReaderSpinWait.getReader());
    }

    @Override
    public ISynchronousEndpointSession getEndpointSession() {
        return endpointSession;
    }

    public void maybeSendHeartbeat() {
        if (lock.tryLock()) {
            try {
                maybeSendHeartbeatLocked();
            } finally {
                lock.unlock();
            }
        }
    }

    private void maybeSendHeartbeatLocked() {
        if (endpointSession.getHeartbeatInterval().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos)) {
            try {
                writeLocked(IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID, -1, -1, EmptyByteBuffer.INSTANCE, false,
                        getDefaultRequestTimeout(), System.nanoTime());
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        if (lock.tryLock()) {
            try {
                closeLocked();
            } finally {
                lock.unlock();
            }
        } else {
            closed = true;
            /*
             * wait for close to finish or at least the lock to be released so that we can close the session
             */
            lock.lock();
            closeLocked();
            lock.unlock();
        }
    }

    private void closeLocked() {
        if (endpointSession != null) {
            //no need to interrupt because we have the lock
            final ScheduledFuture<?> heartbeatFutureCopy = heartbeatFuture;
            if (heartbeatFutureCopy != null) {
                heartbeatFutureCopy.cancel(false);
            }
            try {
                requestWriterSpinWait.getWriter().close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            requestWriterSpinWait = ClosedSynchronousWriter.getSpinWait();
            try {
                responseReaderSpinWait.getReader().close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            responseReaderSpinWait = ClosedSynchronousReader.getSpinWait();
            try {
                endpointSession.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            endpointSession = null;
        }
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Duration getDefaultRequestTimeout() {
        return endpointSession.getRequestTimeout();
    }

    @Override
    public ICloseableByteBufferProvider request(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final boolean closeRequest, final Duration requestTimeout,
            final boolean waitForResponse, final IUnexpectedMessageListener unexpectedMessageListener)
            throws TimeoutException, AbortRequestException {
        lock.lock();
        try {
            final long waitingSinceNanos = System.nanoTime();
            writeLocked(serviceId, methodId, requestSequence, request, closeRequest, requestTimeout, waitingSinceNanos);
            if (!waitForResponse) {
                //fire and forget, another blocking request might receive an answer in the unexpectedMessageListener
                return null;
            }
            while (true) {
                while (!responseReaderSpinWait.hasNext()
                        .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                    if (isClosed()) {
                        throw FastEOFException.getInstance("closed");
                    }
                    if (requestTimeout.isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                        throw FastTimeoutException.getInstance("Request timeout exceeded for [%s:%s:%s]: %s", serviceId,
                                methodId, requestSequence, requestTimeout);
                    }
                }
                try (IServiceSynchronousCommand<IByteBufferProvider> responseHolder = responseReaderSpinWait.getReader()
                        .readMessage()) {
                    final int responseService = responseHolder.getService();
                    final int responseMethod = responseHolder.getMethod();
                    final int responseSequence = responseHolder.getSequence();
                    if (responseSequence != requestSequence) {
                        //ignore invalid response and wait for correct one (might happen due to previous timeout and late response)
                        if (responseSequence < 0) {
                            if (unexpectedMessageListener.onPushedWithoutRequest(this, serviceId, methodId,
                                    responseSequence, request)) {
                                throw new UnsupportedOperationException(
                                        "PushedWithoutRequest messages can not be stored for later polling. Use a "
                                                + MultiplexingSynchronousEndpointClientSession.class.getSimpleName()
                                                + " if this feature is required.");
                            }
                        } else {
                            unexpectedMessageListener.onUnexpectedResponse(this, serviceId, methodId, responseSequence,
                                    request);
                        }
                        continue;
                    }
                    if (responseMethod != methodId) {
                        if (responseMethod == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
                            final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                            final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                            throw new RetryLaterRuntimeException(new RemoteExecutionException(message));
                        } else if (responseMethod == IServiceSynchronousCommand.ERROR_METHOD_ID) {
                            final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                            final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                            throw new RemoteExecutionException(message);
                        } else {
                            throw new RetryLaterRuntimeException("Unexpected methodId in response [" + responseService
                                    + ":" + responseMethod + ":" + responseSequence + "] for request [" + serviceId
                                    + ":" + methodId + ":" + requestSequence + "]");
                        }
                    }
                    if (responseService != serviceId) {
                        throw new RetryLaterRuntimeException("Unexpected serviceId in response [" + responseService
                                + ":" + responseMethod + ":" + responseSequence + "] for request [" + serviceId + ":"
                                + methodId + ":" + requestSequence + "]");
                    }
                    final IByteBufferProvider responseMessage = responseHolder.getMessage();
                    response.setMessage(responseMessage);
                    return response;
                } catch (final RemoteExecutionException | RetryLaterRuntimeException | UnsupportedOperationException
                        | AbortRequestException e) {
                    responseReaderSpinWait.getReader().readFinished();
                    throw e;
                } catch (final Throwable e) {
                    responseReaderSpinWait.getReader().readFinished();
                    throw Throwables.propagate(e);
                }
            }
        } catch (final TimeoutException | RemoteExecutionException | RetryLaterRuntimeException
                | UnsupportedOperationException | AbortRequestException e) {
            lock.unlock();
            throw e;
        } catch (final IOException e) {
            closeLocked();
            lock.unlock();
            throw new RetryLaterRuntimeException(e);
        } catch (final Throwable e) {
            if (Throwables.isCausedByType(e, IOException.class)) {
                closeLocked();
            }
            lock.unlock();
            throw new RetryLaterRuntimeException(e);
        }
    }

    @Override
    public int nextRequestSequence() {
        final int sequence = requestSequenceCounter.incrementAndGet();
        if (sequence < 0) {
            /*
             * specifically synchronizing on atomicInt so that the first inside the lock resets the sequence and all
             * others picking numbers after that
             */
            synchronized (requestSequenceCounter) {
                if (requestSequenceCounter.compareAndSet(sequence, 1)) {
                    return 1;
                } else {
                    return requestSequenceCounter.incrementAndGet();
                }
            }
        } else {
            return sequence;
        }
    }

    @Override
    public int getRequestSequence() {
        return requestSequenceCounter.get();
    }

    @Override
    public void setRequestSequence(final int sequence) {
        requestSequenceCounter.set(sequence);
    }

    @Override
    public int nextStreamSequence() {
        //stream sequence numbers are negative so that the polling queue can separate them properly
        final int sequence = streamSequenceCounter.decrementAndGet();
        if (sequence > 0) {
            /*
             * specifically synchronizing on atomicInt so that the first inside the lock resets the sequence and all
             * others picking numbers after that
             */
            synchronized (streamSequenceCounter) {
                if (streamSequenceCounter.compareAndSet(sequence, -1)) {
                    return -1;
                } else {
                    return streamSequenceCounter.decrementAndGet();
                }
            }
        } else {
            return sequence;
        }
    }

    @Override
    public int getStreamSequence() {
        return streamSequenceCounter.get();
    }

    @Override
    public void setStreamSequence(final int sequence) {
        streamSequenceCounter.set(sequence);
    }

    private void writeLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final boolean closeRequest, final Duration requestTimeout,
            final long waitingSinceNanos) throws Exception, TimeoutException {
        if (request == null) {
            /*
             * nothing to write, must be a subscription from the server that is being polled for, just check if a
             * heartbeat message should be sent instead since heartbeat thread could be locked out constantly
             */
            maybeSendHeartbeatLocked();
            return;
        }
        try {
            requestHolder.setService(serviceId);
            requestHolder.setMethod(methodId);
            requestHolder.setSequence(requestSequence);
            requestHolder.setMessage(request);
            while (!requestWriterSpinWait.writeReady()
                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                if (isClosed()) {
                    throw FastEOFException.getInstance("closed");
                }
                if (requestTimeout.isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                    throw FastTimeoutException.getInstance("Request write ready timeout exceeded for [%s:%s:%s]: %s",
                            serviceId, methodId, requestSequence, requestTimeout);
                }
            }
            requestWriterSpinWait.getWriter().write(requestHolder);
            lastHeartbeatNanos = System.nanoTime();
            while (!requestWriterSpinWait.writeFlushed()
                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                if (isClosed()) {
                    throw FastEOFException.getInstance("closed");
                }
                if (requestTimeout.isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                    throw FastTimeoutException.getInstance("Request write flush timeout exceeded for [%s:%s:%s]: %s",
                            serviceId, methodId, requestSequence, requestTimeout);
                }
            }
        } finally {
            requestHolder.close(); //free memory
            if (closeRequest) {
                final Closeable cRequest = (Closeable) request;
                Closeables.closeQuietly(cRequest);
            }
        }
    }

}
