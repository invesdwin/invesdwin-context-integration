package de.invesdwin.context.integration.channel.rpc.client.session.multi;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient.ClientMethodInfo;
import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponse;
import de.invesdwin.context.integration.channel.rpc.client.session.multi.response.MultiplexingSynchronousEndpointClientSessionResponsePool;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.MutableServiceSynchronousCommand;
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
import de.invesdwin.util.error.MaintenanceIntervalException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
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
    private final AtomicInteger sequenceCounter = new AtomicInteger();
    private final ILock lock;
    @GuardedBy("lock")
    private final ScheduledFuture<?> heartbeatFuture;
    private volatile AtomicBoolean activePolling = new AtomicBoolean();
    private final ManyToOneConcurrentLinkedQueue<MultiplexingSynchronousEndpointClientSessionResponse> writeRequests = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("only the activePolling thread should have access to this map")
    private final Int2ObjectOpenHashMap<MultiplexingSynchronousEndpointClientSessionResponse> writtenRequests = new Int2ObjectOpenHashMap<>();
    private final LoopInterruptedCheck requestWaitIntervalLoopInterruptedCheck;
    private final LoopInterruptedCheck requestTimeoutLoopInterruptedCheck;
    private final ThrottleSpinWait throttle;
    private volatile boolean closed;

    public MultiplexingSynchronousEndpointClientSession(final ISynchronousEndpointSession endpointSession) {
        this.endpointSession = endpointSession;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(MultiplexingSynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriterSpinWait = new SynchronousWriterSpinWait<>(
                endpointSession.newRequestWriter(ByteBufferProviderSerde.GET));
        this.responseReader = endpointSession.newResponseReader(ByteBufferProviderSerde.GET);
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
        this.throttle = new ThrottleSpinWait();
        this.heartbeatFuture = HEARTBEAT_EXECUTOR.scheduleWithFixedDelay(this::maybeSendHeartbeat,
                endpointSession.getHeartbeatInterval().longValue(), endpointSession.getHeartbeatInterval().longValue(),
                endpointSession.getHeartbeatInterval().getTimeUnit().timeUnitValue());
    }

    public void maybeSendHeartbeat() {
        if (lock.tryLock()) {
            try {
                try {
                    if (endpointSession.getHeartbeatInterval().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos)
                            && requestWriterSpinWait.getWriter().writeFlushed()
                            && requestWriterSpinWait.getWriter().writeReady()) {
                        writeAndFlushLocked(IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID, -1, -1,
                                EmptyByteBuffer.INSTANCE);
                    }
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            } finally {
                lock.unlock();
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
    public ICloseableByteBufferProvider request(final ClientMethodInfo methodInfo, final IByteBufferProvider request) {
        final MultiplexingSynchronousEndpointClientSessionResponse response = MultiplexingSynchronousEndpointClientSessionResponsePool.INSTANCE
                .borrowObject();
        response.setOuterActive();
        try {
            final int requestSequence = sequenceCounter.incrementAndGet();
            response.init(methodInfo, request, requestSequence, activePolling);
            if (activePolling.compareAndSet(false, true)) {
                //take over the job of the activePolling thread and handle other requests while polling
                try {
                    return requestActivePolling(response, response);
                } finally {
                    activePolling.set(false);
                }
            } else {
                response.setPollingActive();
                writeRequests.add(response);
                while (true) {
                    if (response.getCompletedSpinWait()
                            .awaitFulfill(System.nanoTime(), endpointSession.getRequestWaitInterval())) {
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
                                return requestActivePolling(response, null);
                            } finally {
                                activePolling.set(false);
                            }
                        }
                    }
                }
            }
        } catch (final Throwable t) {
            response.close();
            throw new RetryLaterRuntimeException(t);
        }
    }

    private MultiplexingSynchronousEndpointClientSessionResponse requestActivePolling(
            final MultiplexingSynchronousEndpointClientSessionResponse outer,
            final MultiplexingSynchronousEndpointClientSessionResponse pollingOuter) throws Exception {
        lock.lock();
        try {
            throttle.outer = outer;
            throttle.pollingOuter = pollingOuter;
            while (true) {
                try {
                    if (!throttle.awaitFulfill(System.nanoTime(), endpointSession.getRequestWaitInterval())) {
                        maybeCheckRequestTimeouts(outer);
                    } else if (outer.isCompleted()) {
                        return outer;
                    }
                } catch (final MaintenanceIntervalException e) {
                    maybeCheckRequestTimeouts(outer);
                }
            }
        } catch (final Throwable t) {
            if (Throwables.isCausedByType(t, IOException.class)) {
                //signal the pool that we want to reconnect
                closeLocked();
            }
            throw Throwables.propagate(t);
        } finally {
            throttle.pollingOuter = null;
            throttle.outer = null;
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

    @SuppressWarnings("resource")
    private void checkRequestTimeouts(final MultiplexingSynchronousEndpointClientSessionResponse outer)
            throws TimeoutException {
        throwIfRequestTimeout(outer);
        if (!writeRequests.isEmpty()) {
            MultiplexingSynchronousEndpointClientSessionResponse writeRequest = writeRequests.peek();
            while (writeRequest != null) {
                if (isRequestTimeout(writeRequest) || !writeRequest.isOuterActive()) {
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
                if (isRequestTimeout(writtenRequest) || !writtenRequest.isOuterActive()) {
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

    private boolean handleLocked(final MultiplexingSynchronousEndpointClientSessionResponse pollingOuter)
            throws Exception {
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
                        //ignore invalid response and wait for correct one (might happen due to previous timeout and late response)
                        return true;
                    }
                }
                if (responseMethod != response.getMethodInfo().getMethodId()) {
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
                        response.responseCompleted(new RetryLaterRuntimeException("Unexpected methodId in response ["
                                + responseService + ":" + responseMethod + ":" + responseSequence + "] for request ["
                                + response.getMethodInfo().getServiceId() + ":" + response.getMethodInfo().getMethodId()
                                + ":" + response.getRequestSequence() + "]"));
                        if (response != pollingOuter) {
                            response.releasePollingActive();
                        }
                        return true;
                    }
                }
                if (responseService != response.getMethodInfo().getServiceId()) {
                    response.responseCompleted(new RetryLaterRuntimeException("Unexpected serviceId in response ["
                            + responseService + ":" + responseMethod + ":" + responseSequence + "] for request ["
                            + response.getMethodInfo().getServiceId() + ":" + response.getMethodInfo().getMethodId()
                            + ":" + response.getRequestSequence() + "]"));
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

    private void writePollingRequest() throws Exception {
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
                        writtenRequests.put(nextWriteTask.getRequestSequence(), nextWriteTask);
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
                    writtenRequests.put(writeTask.getRequestSequence(), writeTask);
                    writeLocked(writeTask);
                }
            }
        }
    }

    private void writeLocked(final MultiplexingSynchronousEndpointClientSessionResponse task) throws Exception {
        try {
            final ClientMethodInfo methodInfo = task.getMethodInfo();
            final int serviceId = methodInfo.getServiceId();
            final int methodId = methodInfo.getMethodId();
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

    private void writeAndFlushLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request) throws Exception {
        writeLocked(serviceId, methodId, requestSequence, request);
        if (!requestWriterSpinWait.writeFlushed()
                .awaitFulfill(System.nanoTime(), endpointSession.getRequestTimeout())) {
            throw new TimeoutException("Request write flush timeout exceeded for [" + serviceId + ":" + methodId + ":"
                    + requestSequence + "]: " + endpointSession.getRequestTimeout());
        }
    }

    private void throwIfRequestTimeout(final MultiplexingSynchronousEndpointClientSessionResponse request)
            throws TimeoutException {
        if (isRequestTimeout(request)) {
            throw new TimeoutException("Request timeout exceeded for [" + request.getMethodInfo().getServiceId() + ":"
                    + request.getMethodInfo().getMethodId() + ":" + request.getRequestSequence() + "]: "
                    + endpointSession.getRequestTimeout());
        }
    }

    private boolean isRequestTimeout(final MultiplexingSynchronousEndpointClientSessionResponse request) {
        return endpointSession.getRequestTimeout()
                .isLessThanOrEqualToNanos(System.nanoTime() - request.getWaitingSinceNanos());
    }

    private class ThrottleSpinWait extends ASpinWait {

        private MultiplexingSynchronousEndpointClientSessionResponse outer;
        private MultiplexingSynchronousEndpointClientSessionResponse pollingOuter;

        @Override
        public boolean isConditionFulfilled() throws Exception {
            //throttle while nothing to do, spin quickly while work is available
            boolean handledOverall = false;
            boolean handledNow;
            do {
                handledNow = handleLocked(pollingOuter);
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

}
