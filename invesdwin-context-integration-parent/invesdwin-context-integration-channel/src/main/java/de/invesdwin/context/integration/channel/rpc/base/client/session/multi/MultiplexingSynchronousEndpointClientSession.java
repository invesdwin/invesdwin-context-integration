package de.invesdwin.context.integration.channel.rpc.base.client.session.multi;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponse;
import de.invesdwin.context.integration.channel.rpc.base.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponsePool;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.AbortRequestException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.iterable.buffer.BufferingIterator;
import de.invesdwin.util.collections.iterable.buffer.IBufferingIterator;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.concurrent.loop.LoopInterruptedCheck;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.error.FastTimeoutException;
import de.invesdwin.util.error.MaintenanceIntervalException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

@ThreadSafe
public class MultiplexingSynchronousEndpointClientSession implements ISynchronousEndpointClientSession {

    @GuardedBy("lock")
    private ISynchronousEndpointSession endpointSession;
    @GuardedBy("lock")
    private SynchronousWriterSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> requestWriterSpinWait;
    @GuardedBy("lock")
    private final MutableServiceSynchronousCommand<IByteBufferProvider> requestHolder = new MutableServiceSynchronousCommand<IByteBufferProvider>();
    @GuardedBy("lock")
    private ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> responseReader;
    @GuardedBy("lock")
    private long lastHeartbeatNanos = System.nanoTime();
    private final AtomicInteger requestSequenceCounter = new AtomicInteger();
    private final AtomicInteger streamSequenceCounter = new AtomicInteger();
    private final ILock lock;
    @GuardedBy("lock")
    private final ScheduledFuture<?> heartbeatFuture;
    private volatile AtomicBoolean activePolling = new AtomicBoolean();
    private final ManyToOneConcurrentLinkedQueue<MultiplexingSynchronousEndpointClientSessionResponse> writeRequests = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("only the activePolling thread should have access to this map")
    private final Int2ObjectOpenHashMap<MultiplexingSynchronousEndpointClientSessionResponse> writtenRequests = new Int2ObjectOpenHashMap<>();
    private final LoopInterruptedCheck requestWaitIntervalLoopInterruptedCheck;
    private final LoopInterruptedCheck requestTimeoutLoopInterruptedCheck;
    private final BlockingHandleLockedSpinWait blockingHandleLockedSpinWait;
    private volatile boolean closed;

    public MultiplexingSynchronousEndpointClientSession(final ISynchronousEndpointSession endpointSession) {
        this.endpointSession = endpointSession;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(MultiplexingSynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriterSpinWait = new SynchronousWriterSpinWait<>(
                endpointSession.newCommandRequestWriter(ByteBufferProviderSerde.GET));
        this.responseReader = endpointSession.newCommandResponseReader(ByteBufferProviderSerde.GET);
        try {
            requestWriterSpinWait.getWriter().open();
            responseReader.open();
        } catch (final Throwable t) {
            closeLocked();
            throw Throwables.propagate(t);
        }
        this.requestWaitIntervalLoopInterruptedCheck = new LoopInterruptedCheck(
                endpointSession.getRequestWaitInterval());
        this.requestTimeoutLoopInterruptedCheck = new LoopInterruptedCheck(endpointSession.getRequestWaitInterval());
        this.blockingHandleLockedSpinWait = new BlockingHandleLockedSpinWait();
        this.heartbeatFuture = HEARTBEAT_EXECUTOR.scheduleWithFixedDelay(this::maybeSendHeartbeat,
                endpointSession.getHeartbeatInterval().longValue(), endpointSession.getHeartbeatInterval().longValue(),
                endpointSession.getHeartbeatInterval().getTimeUnit().timeUnitValue());
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
        try {
            if (endpointSession.getHeartbeatInterval().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos)
                    && requestWriterSpinWait.getWriter().writeFlushed()
                    && requestWriterSpinWait.getWriter().writeReady()) {
                readyWriteAndFlushLocked(IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID, -1, -1,
                        EmptyByteBuffer.INSTANCE, getDefaultRequestTimeout(), System.nanoTime());
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
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
            heartbeatFuture.cancel(false);
            try {
                requestWriterSpinWait.getWriter().close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            requestWriterSpinWait = ClosedSynchronousWriter.getSpinWait();
            try {
                responseReader.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            responseReader = ClosedSynchronousReader.getInstance();
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

    public boolean isHeartbeatTimeout() {
        return closed || endpointSession.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    @Override
    public Duration getDefaultRequestTimeout() {
        return endpointSession.getRequestTimeout();
    }

    private Duration getRequestWaitInterval(final Duration requestTimeout, final boolean waitForResponse) {
        final Duration requestWaitInterval;
        if (waitForResponse) {
            requestWaitInterval = endpointSession.getRequestWaitInterval();
        } else {
            requestWaitInterval = endpointSession.getPollingRequestWaitInterval();
        }
        return requestWaitInterval.orLower(requestTimeout);
    }

    /**
     * Only the active polling request will use its unexpectedMessageListener. All others with only use it if they
     * become the polling request. Otherwise it gets ignored. If multiple requests should be notified, a broadcasting
     * listener should be used for all requests.
     */
    @Override
    public ICloseableByteBufferProvider request(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final boolean closeRequest, final Duration requestTimeout,
            final boolean waitForResponse, final IUnexpectedMessageListener unexpectedMessageListener)
            throws TimeoutException, AbortRequestException {
        final Duration requestWaitInterval = getRequestWaitInterval(requestTimeout, waitForResponse);
        if (!waitForResponse) {
            if (request == null) {
                //just send a heartbeat if needed
                maybeSendHeartbeat();
                return null;
            }
            //try to write immediately, otherwise add to queue
            if (writeRequests.isEmpty() && lock.tryLock()) {
                try {
                    if (requestWriterSpinWait.getWriter().writeFlushed()
                            && requestWriterSpinWait.getWriter().writeReady()) {
                        //fire and forget, another blocking request might receive an answer in the unexpectedMessageListener
                        pollingReadyWriteAndFlushLocked(serviceId, methodId, requestSequence, request, requestTimeout,
                                requestWaitInterval, System.nanoTime(), unexpectedMessageListener);
                        return null;
                    }
                } catch (RemoteExecutionException | RetryLaterRuntimeException e) {
                    throw e;
                } catch (final Throwable t) {
                    throw new RetryLaterRuntimeException(t);
                } finally {
                    lock.unlock();
                    if (closeRequest) {
                        final Closeable cRequest = (Closeable) request;
                        Closeables.closeQuietly(cRequest);
                    }
                }
            }
        }
        final MultiplexingSynchronousEndpointClientSessionResponse response = MultiplexingSynchronousEndpointClientSessionResponsePool.INSTANCE
                .borrowObject();
        response.setOuterActive();
        try {
            response.init(serviceId, methodId, requestSequence, request, closeRequest, requestTimeout,
                    requestWaitInterval, activePolling);
            if (!waitForResponse) {
                //since we could not write it immediately, add it to the queue
                response.setPushedWithoutRequest();
                writeRequests.add(response);
                /*
                 * fire and forget, another blocking request might receive an answer in the unexpectedMessageListener
                 * 
                 * though if there is no other request, we have to poll non-blocking for potential responses so that the
                 * response buffer does not stay full
                 */
                maybePollForResponsesNonBlocking(unexpectedMessageListener);
                return null;
            } else {
                return waitForResponseBlocking(unexpectedMessageListener, response);
            }
        } catch (final TimeoutException | RemoteExecutionException | RetryLaterRuntimeException
                | AbortRequestException e) {
            response.close();
            throw e;
        } catch (final Throwable t) {
            response.close();
            throw new RetryLaterRuntimeException(t);
        }
    }

    protected void maybePollForResponsesNonBlocking(final IUnexpectedMessageListener unexpectedMessageListener)
            throws Exception, FastEOFException, TimeoutException {
        if (lock.tryLock()) {
            try {
                pollForResponsesNonBlockingLocked(unexpectedMessageListener);
            } finally {
                lock.unlock();
            }
        }
    }

    private ICloseableByteBufferProvider waitForResponseBlocking(
            final IUnexpectedMessageListener unexpectedMessageListener,
            final MultiplexingSynchronousEndpointClientSessionResponse response)
            throws Exception, FastEOFException, TimeoutException {
        if (activePolling.compareAndSet(false, true)) {
            //take over the job of the activePolling thread and handle other requests while polling
            try {
                return requestActivePollingBlocking(unexpectedMessageListener, response, response);
            } finally {
                activePolling.set(false);
            }
        } else {
            response.setPollingActive();
            if (!response.isPollOnly()) {
                writeRequests.add(response);
            } else if (response.getRequest() != null) {
                throw new IllegalArgumentException("request should be null for pollOnly=true requests");
            }
            while (true) {
                if (response.getCompletedSpinWait()
                        .awaitFulfill(System.nanoTime(), response.getRequestWaitInterval())) {
                    if (response.isCompleted()) {
                        //the other thread finished our work for us
                        return response;
                    }
                    if (isClosed()) {
                        throw FastEOFException.getInstance("closed");
                    }
                    throwIfRequestTimeout(response);
                    if (activePolling.compareAndSet(false, true)) {
                        //take over the job of the other activePolling thread that just go finished
                        try {
                            return requestActivePollingBlocking(unexpectedMessageListener, response, null);
                        } finally {
                            activePolling.set(false);
                        }
                    }
                }
            }
        }
    }

    private void pollForResponsesNonBlockingLocked(final IUnexpectedMessageListener unexpectedMessageListener)
            throws Exception, FastEOFException, TimeoutException {
        if (activePolling.compareAndSet(false, true)) {
            //take over the job of the activePolling thread and handle other requests while polling
            try {
                maybeCheckOtherRequestTimeouts();
                boolean handled;
                do {
                    handled = handleLocked(unexpectedMessageListener, null);
                } while (handled || !writeRequests.isEmpty());
            } finally {
                activePolling.set(false);
            }
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

    private MultiplexingSynchronousEndpointClientSessionResponse requestActivePollingBlocking(
            final IUnexpectedMessageListener unexpectedMessageListener,
            final MultiplexingSynchronousEndpointClientSessionResponse outer,
            final MultiplexingSynchronousEndpointClientSessionResponse pollingOuter) throws Exception {
        lock.lock();
        if (pollingOuter != null && pollingOuter.getRequest() == null) {
            /*
             * nothing to write, must be a subscription from the server that is being polled for, just check if a
             * heartbeat message should be sent instead since heartbeat thread could be locked out constantly
             */
            maybeSendHeartbeatLocked();
        }
        try {
            blockingHandleLockedSpinWait.unexpectedMessageListener = unexpectedMessageListener;
            blockingHandleLockedSpinWait.outer = outer;
            blockingHandleLockedSpinWait.pollingOuter = pollingOuter;
            while (true) {
                try {
                    if (!blockingHandleLockedSpinWait.awaitFulfill(System.nanoTime(), outer.getRequestWaitInterval())) {
                        maybeCheckRequestTimeouts(outer);
                    } else if (outer.isCompleted()) {
                        return outer;
                    }
                } catch (final MaintenanceIntervalException e) {
                    maybeCheckRequestTimeouts(outer);
                }
            }
        } catch (final TimeoutException | RemoteExecutionException | RetryLaterRuntimeException
                | AbortRequestException e) {
            throw e;
        } catch (final IOException e) {
            //signal the pool that we want to reconnect
            closeLocked();
            throw new RetryLaterRuntimeException(e);
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, IOException.class)) {
                //signal the pool that we want to reconnect
                closeLocked();
            }
            throw new RetryLaterRuntimeException(t);
        } finally {
            blockingHandleLockedSpinWait.pollingOuter = null;
            blockingHandleLockedSpinWait.outer = null;
            blockingHandleLockedSpinWait.unexpectedMessageListener = null;
            lock.unlock();
        }
    }

    private void maybeCheckRequestTimeouts(final MultiplexingSynchronousEndpointClientSessionResponse outer)
            throws InterruptedException, FastEOFException, TimeoutException {
        //only check heartbeat interval when there is no more work or when the requestWaitInterval is reached
        if (requestTimeoutLoopInterruptedCheck.check()) {
            if (isClosed()) {
                closeLocked();
                throw FastEOFException.getInstance("closed");
            }
            checkRequestTimeouts(outer);
        }
    }

    private void maybeCheckOtherRequestTimeouts() throws InterruptedException, FastEOFException, TimeoutException {
        //only check heartbeat interval when there is no more work or when the requestWaitInterval is reached
        if (requestTimeoutLoopInterruptedCheck.check()) {
            if (isClosed()) {
                closeLocked();
                throw FastEOFException.getInstance("closed");
            }
            checkOtherRequestTimeouts();
        }
    }

    @SuppressWarnings("resource")
    private void checkRequestTimeouts(final MultiplexingSynchronousEndpointClientSessionResponse outer)
            throws TimeoutException {
        throwIfRequestTimeout(outer);
        checkOtherRequestTimeouts();
    }

    private void checkOtherRequestTimeouts() {
        if (!writeRequests.isEmpty()) {
            MultiplexingSynchronousEndpointClientSessionResponse writeRequest = writeRequests.peek();
            while (writeRequest != null) {
                if (writeRequest.isRequestTimeout() || !writeRequest.isOuterActive()) {
                    final MultiplexingSynchronousEndpointClientSessionResponse removed = writeRequests.remove();
                    assert writeRequest == removed;
                    removed.releasePollingActive();
                    writeRequest = writeRequests.peek();
                } else {
                    writeRequest = null;
                }
            }
        }
        if (!writtenRequests.isEmpty()) {
            IBufferingIterator<MultiplexingSynchronousEndpointClientSessionResponse> toBeRemoved = null;
            for (final MultiplexingSynchronousEndpointClientSessionResponse writtenRequest : writtenRequests.values()) {
                if (writtenRequest.isRequestTimeout() || !writtenRequest.isOuterActive()) {
                    if (toBeRemoved == null) {
                        toBeRemoved = new BufferingIterator<>();
                    }
                    toBeRemoved.add(writtenRequest);
                }
            }
            if (toBeRemoved != null) {
                try {
                    while (true) {
                        final MultiplexingSynchronousEndpointClientSessionResponse next = toBeRemoved.next();
                        writtenRequests.remove(next.getRequestSequence());
                        next.releasePollingActive();
                    }
                } catch (final NoSuchElementException e) {
                    //end reached
                }
            }
        }
    }

    private boolean handleLocked(final IUnexpectedMessageListener unexpectedMessageListener,
            final MultiplexingSynchronousEndpointClientSessionResponse pollingOuter)
            throws IOException, AbortRequestException {
        final boolean writing;
        if ((pollingOuter != null && pollingOuter.getRequest() != null || !writeRequests.isEmpty())
                && requestWriterSpinWait.getWriter().writeFlushed() && requestWriterSpinWait.getWriter().writeReady()) {
            /*
             * even if we finish writing the current task and no other task follows, we still handled something and
             * continue eagerly looking for more work (client might immediately give us a new task)
             */
            writing = true;
            if (pollingOuter != null && pollingOuter.getRequest() != null) {
                if (pollingOuter.isWritingActive()) {
                    pollingOuter.releaseWritingActive();
                } else {
                    pollingOuter.setWritingActive();
                    writeLocked(pollingOuter);
                }
            } else {
                writePollingRequest();
            }
        } else {
            //reading could still indicate that we are busy handling work
            writing = false;
        }
        if (responseReader.hasNext()) {
            try (IServiceSynchronousCommand<IByteBufferProvider> responseHolder = responseReader.readMessage()) {
                final int responseService = responseHolder.getService();
                final int responseMethod = responseHolder.getMethod();
                final int responseSequence = responseHolder.getSequence();
                final MultiplexingSynchronousEndpointClientSessionResponse response;
                if (pollingOuter != null && responseSequence == pollingOuter.getRequestSequence()) {
                    response = pollingOuter;
                } else {
                    response = writtenRequests.remove(responseSequence);
                    if (response == null) {
                        maybeAddPushedWithoutRequest(unexpectedMessageListener, responseService, responseMethod,
                                responseSequence, responseHolder.getMessage());
                        /*
                         * ignore invalid response and wait for correct one (might happen due to previous timeout and
                         * late response or pushed streaming message)
                         */
                        return true;
                    }
                }
                if (responseMethod != response.getMethodId()) {
                    if (responseMethod == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
                        final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                        final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                        response.responseCompleted(
                                new RetryLaterRuntimeException(new RemoteExecutionException(message)));
                        if (response != pollingOuter) {
                            response.releasePollingActive();
                        }
                        return true;
                    } else if (responseMethod == IServiceSynchronousCommand.ERROR_METHOD_ID) {
                        final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                        final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                        response.responseCompleted(new RemoteExecutionException(message));
                        if (response != pollingOuter) {
                            response.releasePollingActive();
                        }
                        return true;
                    } else {
                        response.responseCompleted(new RetryLaterRuntimeException(
                                "Unexpected methodId in response [" + responseService + ":" + responseMethod + ":"
                                        + responseSequence + "] for request [" + response.getServiceId() + ":"
                                        + response.getMethodId() + ":" + response.getRequestSequence() + "]"));
                        if (response != pollingOuter) {
                            response.releasePollingActive();
                        }
                        return true;
                    }
                }
                if (responseService != response.getServiceId()) {
                    response.responseCompleted(new RetryLaterRuntimeException(
                            "Unexpected serviceId in response [" + responseService + ":" + responseMethod + ":"
                                    + responseSequence + "] for request [" + response.getServiceId() + ":"
                                    + response.getMethodId() + ":" + response.getRequestSequence() + "]"));
                    if (response != pollingOuter) {
                        response.releasePollingActive();
                    }
                    return true;
                }
                final IByteBufferProvider responseMessage = responseHolder.getMessage();
                response.responseCompleted(responseMessage);
                if (response != pollingOuter) {
                    response.releasePollingActive();
                }
                return true;
            } finally {
                responseReader.readFinished();
            }
        }
        return writing;
    }

    private void maybeAddPushedWithoutRequest(final IUnexpectedMessageListener unexpectedMessageListener,
            final int responseService, final int responseMethod, final int responseSequence,
            final IByteBufferProvider responseMessage) throws IOException, AbortRequestException {
        if (responseSequence < 0) {
            if (unexpectedMessageListener.onPushedWithoutRequest(this, responseService, responseMethod,
                    responseSequence, responseMessage)) {
                /*
                 * this might be a streaming message that we should add so that it can be polled for from the outside
                 * later (at least until requestTimeout is exceeded)
                 */
                final MultiplexingSynchronousEndpointClientSessionResponse pushedWithoutRequest = MultiplexingSynchronousEndpointClientSessionResponsePool.INSTANCE
                        .borrowObject();
                pushedWithoutRequest.init(responseService, responseMethod, responseSequence, null, false,
                        getDefaultRequestTimeout(), null, activePolling);
                pushedWithoutRequest.setPushedWithoutRequest();
                pushedWithoutRequest.responseCompleted(responseMessage);
                writtenRequests.put(responseSequence, pushedWithoutRequest);
            }
        } else {
            unexpectedMessageListener.onUnexpectedResponse(this, responseService, responseMethod, responseSequence,
                    responseMessage);
        }
    }

    private void writePollingRequest() throws IOException {
        final MultiplexingSynchronousEndpointClientSessionResponse writeTask = writeRequests.peek();
        if (writeTask != null) {
            if (writeTask.isWritingActive()) {
                final MultiplexingSynchronousEndpointClientSessionResponse removedTask = writeRequests.remove();
                assert writeTask == removedTask;
                writeTask.releaseWritingActive();
                final MultiplexingSynchronousEndpointClientSessionResponse nextWriteTask = writeRequests.peek();
                if (nextWriteTask != null) {
                    if (!nextWriteTask.isOuterActive()) {
                        final MultiplexingSynchronousEndpointClientSessionResponse removed = writeRequests.remove();
                        assert nextWriteTask == removed;
                        nextWriteTask.releasePollingActive();
                    } else {
                        //make sure this is marked as written before a response could be received
                        nextWriteTask.setWritingActive();
                        putWrittenRequest(nextWriteTask);
                        writeLocked(nextWriteTask);
                    }
                }
            } else {
                if (!writeTask.isOuterActive()) {
                    final MultiplexingSynchronousEndpointClientSessionResponse removed = writeRequests.remove();
                    assert writeTask == removed;
                    writeTask.releasePollingActive();
                } else {
                    //make sure this is marked as written before a response could be received
                    writeTask.setWritingActive();
                    putWrittenRequest(writeTask);
                    writeLocked(writeTask);
                }
            }
        }
    }

    private void putWrittenRequest(final MultiplexingSynchronousEndpointClientSessionResponse writeTask) {
        if (writeTask.isPushedWithoutRequest()) {
            //fire and forget, another blocking request might receive an answer in the unexpectedMessageListener
            return;
        }
        final int requestSequence = writeTask.getRequestSequence();
        final MultiplexingSynchronousEndpointClientSessionResponse removed = writtenRequests.put(requestSequence,
                writeTask);
        if (removed != null && removed.isPushedWithoutRequest()) {
            writeTask.maybeResponseCompleted(removed);
            removed.close();
        }
    }

    private void writeLocked(final MultiplexingSynchronousEndpointClientSessionResponse task) throws IOException {
        if (task.getRequest() == null) {
            //nothing to write, must be a subscription from the server that is being polled for
            return;
        }
        try {
            final int serviceId = task.getServiceId();
            final int methodId = task.getMethodId();
            final int requestSequence = task.getRequestSequence();
            final IByteBufferProvider request = task.getRequest();
            writeLocked(serviceId, methodId, requestSequence, request);
        } finally {
            requestHolder.close(); //free memory
        }
    }

    private void writeLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request) throws IOException {
        requestHolder.setService(serviceId);
        requestHolder.setMethod(methodId);
        requestHolder.setSequence(requestSequence);
        requestHolder.setMessage(request);
        requestWriterSpinWait.getWriter().write(requestHolder);
        lastHeartbeatNanos = System.nanoTime();
    }

    private void readyWriteAndFlushLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final Duration requestTimeout, final long waitingSinceNanos)
            throws Exception {
        try {
            writeLocked(serviceId, methodId, requestSequence, request);
            if (!requestWriterSpinWait.writeFlushed().awaitFulfill(System.nanoTime(), requestTimeout)) {
                throw FastTimeoutException.getInstance("Request write flush timeout exceeded for [%s:%s:%s]: %s",
                        serviceId, methodId, requestSequence, request);
            }
        } finally {
            requestHolder.close(); //free memory
        }
    }

    private void pollingReadyWriteAndFlushLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final Duration requestTimeout, final Duration requestWaitInterval,
            final long waitingSinceNanos, final IUnexpectedMessageListener unexpectedMessageListener) throws Exception {
        try {
            writeLocked(serviceId, methodId, requestSequence, request);
            if (!requestWriterSpinWait.getWriter().writeFlushed()) {
                pollForResponsesNonBlockingLocked(unexpectedMessageListener);
            }
            while (!requestWriterSpinWait.writeFlushed().awaitFulfill(System.nanoTime(), requestWaitInterval)) {
                if (requestTimeout.isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                    throw FastTimeoutException.getInstance("Request write flush timeout exceeded for [%s:%s:%s]: %s",
                            serviceId, methodId, requestSequence, requestTimeout);
                }
                pollForResponsesNonBlockingLocked(unexpectedMessageListener);
            }
        } finally {
            requestHolder.close(); //free memory
        }
    }

    private void throwIfRequestTimeout(final MultiplexingSynchronousEndpointClientSessionResponse request)
            throws TimeoutException {
        if (request.isRequestTimeout()) {
            throw FastTimeoutException.getInstance("Request timeout exceeded for [%s:%s:%s]: %s",
                    request.getServiceId(), request.getMethodId(), request.getRequestSequence(),
                    request.getRequestTimeout());
        }
    }

    private final class BlockingHandleLockedSpinWait extends ASpinWait {

        private MultiplexingSynchronousEndpointClientSessionResponse outer;
        private MultiplexingSynchronousEndpointClientSessionResponse pollingOuter;
        private IUnexpectedMessageListener unexpectedMessageListener;

        @Override
        public boolean isConditionFulfilled() throws Exception {
            //throttle while nothing to do, spin quickly while work is available
            boolean handledOverall = false;
            boolean handledNow;
            do {
                handledNow = handleLocked(unexpectedMessageListener, pollingOuter);
                handledOverall |= handledNow;
                if (outer.isCompleted()) {
                    return true;
                }
                if (requestWaitIntervalLoopInterruptedCheck.check()) {
                    //maybe check request timeout
                    throw MaintenanceIntervalException.getInstance("check request timeout");
                }
            } while (handledNow);
            return handledOverall;
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(endpointSession).toString();
    }

}
