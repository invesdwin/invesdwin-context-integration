package de.invesdwin.context.integration.channel.stream.server.session;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResultPool;
import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableSet;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Allows to process multiple requests in parallel for the same endpoint by multiplexing it.
 */
@ThreadSafe
public class MultiplexingStreamSynchronousEndpointServerSession
        implements ISynchronousEndpointServerSession, IStreamSynchronousEndpointSession {

    private final IStreamSynchronousEndpointServer server;
    private ISynchronousEndpointSession endpointSession;
    private final String sessionId;
    private final Duration heartbeatTimeout;
    private final Duration requestTimeout;
    private ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> requestReader;
    private ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> responseWriter;
    @GuardedBy("volatile not needed because the same request runnable thread writes and reads this field only")
    private long lastHeartbeatNanos = System.nanoTime();
    /*
     * We don't use a bounded sized queue here because one slow client might otherwise block worker threads that could
     * still work on other tasks for other clients. PendingCount check will just
     */
    private final ManyToOneConcurrentLinkedQueue<ProcessResponseResult> writeQueue = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("only the io thread should access the result pool")
    private final NodeBufferingIterator<ProcessResponseResult> pollingQueue = new NodeBufferingIterator<>();
    private final ManyToOneConcurrentLinkedQueue<ProcessResponseResult> pollingQueueAsyncAdds = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("only the io thread should access the result pool")
    private final IFastIterableSet<ProcessResponseResult> activeRequests = ILockCollectionFactory.getInstance(false)
            .newFastIterableIdentitySet();
    private final IStreamSessionManager manager;
    private int skipRequestReadingCount = 0;
    private int streamSequenceCounter = 0;

    public MultiplexingStreamSynchronousEndpointServerSession(final IStreamSynchronousEndpointServer server,
            final ISynchronousEndpointSession endpointSession) {
        this.server = server;
        this.manager = server.newManager(this);
        this.endpointSession = endpointSession;
        this.sessionId = endpointSession.getSessionId();
        this.heartbeatTimeout = endpointSession.getHeartbeatTimeout();
        this.requestTimeout = endpointSession.getRequestTimeout();
        this.requestReader = endpointSession.newCommandRequestReader(ByteBufferProviderSerde.GET);
        this.responseWriter = endpointSession.newCommandResponseWriter(ByteBufferProviderSerde.GET);
        try {
            requestReader.open();
            responseWriter.open();
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
    }

    @Override
    public IStreamSynchronousEndpointServer getServer() {
        return server;
    }

    @Override
    public void close() {
        //only the IO thread will access the active requests array, also only IO thread will call close of the server session, thus we are fine here
        final ProcessResponseResult[] activeRequestsArray = activeRequests.asArray(ProcessResponseResult.EMPTY_ARRAY);
        for (int i = 0; i < activeRequestsArray.length; i++) {
            final ProcessResponseResult activeRequest = activeRequestsArray[i];
            final Future<?> futureCopy = activeRequest.getFuture();
            if (futureCopy != null) {
                activeRequest.setFuture(null);
                futureCopy.cancel(true);
            }
        }
        activeRequests.clear();
        try {
            manager.close();
        } catch (final Throwable t) {
            Err.process(new RuntimeException("Ignoring", t));
        }
        final ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> requestReaderCopy = requestReader;
        requestReader = ClosedSynchronousReader.getInstance();
        try {
            requestReaderCopy.close();
        } catch (final Throwable t) {
            Err.process(new RuntimeException("Ignoring", t));
        }
        final ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> responseWriterCopy = responseWriter;
        responseWriter = ClosedSynchronousWriter.getInstance();
        try {
            responseWriterCopy.close();
        } catch (final Throwable t) {
            Err.process(new RuntimeException("Ignoring", t));
        }
        final ISynchronousEndpointSession endpointSessionCopy = endpointSession;
        if (endpointSessionCopy != null) {
            try {
                endpointSessionCopy.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            endpointSession = null;
        }
    }

    @Override
    public boolean isClosed() {
        return endpointSession == null;
    }

    @Override
    public boolean pushSubscriptionMessage(final IStreamSynchronousEndpointService service,
            final ISynchronousReader<IByteBufferProvider> reader) throws IOException {
        if (!responseWriter.writeReady()) {
            //writer is not ready, continue with another session
            throw FastNoSuchElementException.getInstance("writer.writeReady is false");
        }
        if (!responseWriter.writeFlushed()) {
            //writer has not yet flushed, continue with another session
            throw FastNoSuchElementException.getInstance("writer.writeFlushed is false");
        }
        if (writeQueue.size() > server.getMaxSuccessivePushCountPerSession()) {
            //a request processing is still active which should be handled by the handleRequests method
            throw FastNoSuchElementException.getInstance("writeQueue.size exceeds maxSuccessivePushCountPerSession");
        }
        final ProcessResponseResult writeTask = ProcessResponseResultPool.INSTANCE.borrowObject();
        final IByteBufferProvider message = reader.readMessage();
        writeTask.getResponse().setService(service.getServiceId());
        writeTask.getResponse().setMethod(StreamServerMethodInfo.METHOD_ID_PUSH);
        /*
         * add a sequence to the pushed messages so that the client can validate if he missed some messages and
         * re-request them by resubscribing with his last known timestamp as a limiter in the subscription request or by
         * resetting the subscription entirely. The stream sequence numbers are negative so that they be separated in a
         * polling queues from rpc requests/responses.
         */
        final int sequence = nextStreamSequence();
        writeTask.getResponse().setSequence(sequence);
        writeTask.getResponse().setMessageBuffer(message);
        responseWriter.write(writeTask.getResponse());
        final boolean flushed = responseWriter.writeFlushed();
        if (flushed) {
            writeTask.getResponse().close();
            writeTask.close();
            reader.readFinished();
        } else {
            //let handleRequests flush the message
            writeTask.setReadFinishedReader(reader);
            writeTask.setWriting(true);
            writeQueue.add(writeTask);
        }
        return flushed;
    }

    private int nextStreamSequence() {
        final int sequence = --streamSequenceCounter;
        if (sequence > 0) {
            //handle rollover
            streamSequenceCounter = -1;
            return -1;
        } else {
            return sequence;
        }
    }

    //handling requests has a higher priority than handling subscriptions, except for bursts from subscriptions
    @Override
    public boolean handle() throws IOException {
        final boolean requestHandled = handleRequests();
        if (requestHandled) {
            return true;
        } else {
            final boolean managerHandled = manager.handle();
            if (managerHandled) {
                if (skipRequestReadingCount == 0) {
                    //give pushing messages priority
                    skipRequestReadingCount = server.getMaxSuccessivePushCountPerSession();
                } else {
                    //decrease priority for pushing messages
                    skipRequestReadingCount--;
                }
            } else {
                //we can check for requests again now
                skipRequestReadingCount = 0;
            }
            return managerHandled;
        }
    }

    private boolean handleRequests() throws IOException {
        maybePollResults();
        final boolean writing;
        final ProcessResponseResult writeTask = writeQueue.peek();
        if (writeTask != null) {
            /*
             * even if we finish writing the current task and no other task follows, we still handled something and
             * continue eagerly looking for more work (client might immediately give us a new task)
             */
            writing = true;
            if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                if (writeTask.isWriting()) {
                    final ProcessResponseResult removedTask = writeQueue.remove();
                    final ProcessResponseResult nextWriteTask = writeQueue.peek();
                    if (nextWriteTask != null) {
                        responseWriter.write(nextWriteTask.getResponse());
                        nextWriteTask.setWriting(true);
                    }
                    activeRequests.remove(writeTask);
                    writeTask.close();
                    Assertions.checkSame(writeTask, removedTask);
                } else {
                    responseWriter.write(writeTask.getResponse());
                    writeTask.setWriting(true);
                }
            }
        } else {
            //reading could still indicate that we are busy handling work
            writing = false;
        }
        if (skipRequestReadingCount > 0) {
            return false;
        }
        try {
            if (requestReader.hasNext()) {
                lastHeartbeatNanos = System.nanoTime();

                final ProcessResponseResult result = ProcessResponseResultPool.INSTANCE.borrowObject();
                try {
                    activeRequests.add(result);
                    dispatchProcessResponse(result);
                } catch (final Throwable t) {
                    activeRequests.remove(result);
                    result.close();
                    throw Throwables.propagate(t);
                }
                return true;
            } else {
                return writing;
            }
        } catch (final IOException e) {
            close();
            return false;
        }
    }

    private void maybePollResults() {
        if (!pollingQueueAsyncAdds.isEmpty()) {
            ProcessResponseResult addPollingResult = pollingQueueAsyncAdds.poll();
            while (addPollingResult != null) {
                pollingQueue.add(addPollingResult);
                addPollingResult = pollingQueueAsyncAdds.poll();
            }
        }
        if (!pollingQueue.isEmpty()) {
            ProcessResponseResult pollingResult = pollingQueue.getHead();
            while (pollingResult != null) {
                final ProcessResponseResult nextPollingResult = pollingResult.getNext();
                if (pollingResult.isDone()) {
                    if (pollingResult.isDelayedWriteResponse()) {
                        final EagerSerializingServiceSynchronousCommand<Object> response = pollingResult.getResponse();
                        if (response.hasMessage()) {
                            writeQueue.add(pollingResult);
                        }
                    }
                    pollingQueue.remove(pollingResult);
                }
                pollingResult = nextPollingResult;
            }
        }
    }

    @Override
    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public boolean isHeartbeatTimeout() {
        return heartbeatTimeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    /**
     * This is not measured based on individual requests, instead it is measured based on the handler/session being
     * still active.
     */
    private boolean isRequestTimeout() {
        if (isClosed()) {
            return true;
        }
        return requestTimeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private void dispatchProcessResponse(final ProcessResponseResult result) throws IOException {
        final IServiceSynchronousCommand<IByteBufferProvider> request = requestReader.readMessage();
        try {
            final int serviceId = request.getService();
            if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
                return;
            }
            final int methodId = request.getMethod();
            final StreamServerMethodInfo methodInfo = StreamServerMethodInfo.valueOfNullable(methodId);
            if (methodInfo == null) {
                final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
                response.setService(serviceId);
                response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
                response.setSequence(request.getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "method not found: " + methodId);
                writeQueue.add(result);
                return;
            }

            final WrappedExecutorService workExecutor = server.getWorkExecutor();
            if (workExecutor == null || methodInfo.isBlocking()) {
                final Future<Object> future = methodInfo.invoke(manager, sessionId, request, result.getResponse());
                if (future != null && !future.isDone()) {
                    result.setFuture(future);
                    result.setDelayedWriteResponse(true);
                    pollingQueue.add(result);
                } else {
                    if (result.getResponse().hasMessage()) {
                        writeQueue.add(result);
                    }
                }
            } else {
                final int maxPendingWorkCountPerSession = server.getMaxPendingWorkCountPerSession();
                if (maxPendingWorkCountPerSession > 0) {
                    final int thisPendingCount = activeRequests.size();
                    if (thisPendingCount > maxPendingWorkCountPerSession) {
                        final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
                        response.setService(serviceId);
                        response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                        response.setSequence(request.getSequence());
                        response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                                "too many requests pending for this session [" + thisPendingCount
                                        + "], please try again later");
                        writeQueue.add(result);
                        return;
                    }
                }
                final int maxPendingWorkCountOverall = server.getMaxPendingWorkCountOverall();
                if (maxPendingWorkCountOverall > 0) {
                    final int overallPendingCount = workExecutor.getPendingCount();
                    if (overallPendingCount > maxPendingWorkCountOverall) {
                        final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
                        response.setService(serviceId);
                        response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                        response.setSequence(request.getSequence());
                        response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                                "too many requests pending overall [" + overallPendingCount
                                        + "], please try again later");
                        writeQueue.add(result);
                        return;
                    }
                }
                //copy request for the async processing
                result.getRequestCopy().copy(request);
                result.setFuture(workExecutor.submit(new ProcessResponseTask(methodInfo, result)));
            }
        } finally {
            requestReader.readFinished();
        }
    }

    private final class ProcessResponseTask implements Callable<Object> {
        private final StreamServerMethodInfo methodInfo;
        private final ProcessResponseResult result;

        private ProcessResponseTask(final StreamServerMethodInfo methodInfo, final ProcessResponseResult result) {
            this.methodInfo = methodInfo;
            this.result = result;
        }

        @Override
        public Object call() {
            final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
            if (isClosed()) {
                return null;
            } else if (isRequestTimeout()) {
                response.setService(result.getRequestCopy().getService());
                response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                response.setSequence(result.getRequestCopy().getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "request timeout [" + requestTimeout + "] exceeded, please try again later");
                writeQueue.add(result);
                return null;
            }
            final Future<Object> future = methodInfo.invoke(manager, sessionId, result.getRequestCopy(), response);
            if (future != null && !future.isDone()) {
                result.setDelayedWriteResponse(true);
                pollingQueueAsyncAdds.add(result);
                return future;
            } else {
                if (response.hasMessage()) {
                    writeQueue.add(result);
                }
                return null;
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(endpointSession).toString();
    }

}
