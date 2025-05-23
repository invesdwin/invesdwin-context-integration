package de.invesdwin.context.integration.channel.rpc.base.server.session;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.RpcSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResultPool;
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
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Allows to process multiple requests in parallel for the same endpoint by multiplexing it.
 */
@ThreadSafe
public class MultiplexingRpcSynchronousEndpointServerSession implements ISynchronousEndpointServerSession {

    private final RpcSynchronousEndpointServer parent;
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

    public MultiplexingRpcSynchronousEndpointServerSession(final RpcSynchronousEndpointServer parent,
            final ISynchronousEndpointSession endpointSession) {
        this.parent = parent;
        this.endpointSession = endpointSession;
        Assertions.checkNotNull(endpointSession);
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
    public void close() {
        if (isClosed()) {
            return;
        }
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
    public boolean handle() throws IOException {
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
                        final ISerializingServiceSynchronousCommand<Object> response = pollingResult.getResponse();
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
            final RpcSynchronousEndpointService service = parent.getService(serviceId);
            if (service == null) {
                final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
                response.setService(serviceId);
                response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
                response.setSequence(request.getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "service not found: " + serviceId);
                writeQueue.add(result);
                return;
            }
            final int methodId = request.getMethod();
            final RpcServerMethodInfo methodInfo = service.getMethodInfo(methodId);
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

            final WrappedExecutorService workExecutor = parent.getWorkExecutor();
            if (workExecutor == null || methodInfo.isBlocking()) {
                final Future<Object> future = methodInfo.invoke(sessionId, request, result.getResponse());
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
                final int maxPendingWorkCountPerSession = parent.getMaxPendingWorkCountPerSession();
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
                final int maxPendingWorkCountOverall = parent.getMaxPendingWorkCountOverall();
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
        private final RpcServerMethodInfo methodInfo;
        private final ProcessResponseResult result;

        private ProcessResponseTask(final RpcServerMethodInfo methodInfo, final ProcessResponseResult result) {
            this.methodInfo = methodInfo;
            this.result = result;
        }

        @Override
        public Object call() {
            final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
            if (isClosed()) {
                return null;
            } else if (isRequestTimeout()) {
                response.setService(methodInfo.getService().getServiceId());
                response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                response.setSequence(result.getRequestCopy().getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "request timeout [" + requestTimeout + "] exceeded, please try again later");
                writeQueue.add(result);
                return null;
            }
            final Future<Object> future = methodInfo.invoke(sessionId, result.getRequestCopy(), response);
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
