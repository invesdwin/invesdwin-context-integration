package de.invesdwin.context.integration.channel.rpc.server.session;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService.ServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResultPool;
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
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Allows to process multiple requests in parallel for the same endpoint by multiplexing it.
 */
@ThreadSafe
public class MultiplexingSynchronousEndpointServerSession implements ISynchronousEndpointServerSession {

    private final SynchronousEndpointServer parent;
    private ISynchronousEndpointSession endpointSession;
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

    public MultiplexingSynchronousEndpointServerSession(final SynchronousEndpointServer parent,
            final ISynchronousEndpointSession endpointSession) {
        this.parent = parent;
        this.endpointSession = endpointSession;
        this.requestReader = endpointSession.newRequestReader(ByteBufferProviderSerde.GET);
        this.responseWriter = endpointSession.newResponseWriter(ByteBufferProviderSerde.GET);
        try {
            requestReader.open();
            responseWriter.open();
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
    }

    @Override
    public ISynchronousEndpointSession getEndpointSession() {
        return endpointSession;
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
                    ProcessResponseResultPool.INSTANCE.returnObject(writeTask);
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
        if (requestReader.hasNext()) {
            lastHeartbeatNanos = System.nanoTime();

            final ProcessResponseResult result = ProcessResponseResultPool.INSTANCE.borrowObject();
            try {
                activeRequests.add(result);
                dispatchProcessResponse(result);
            } catch (final EOFException e) {
                activeRequests.remove(result);
                ProcessResponseResultPool.INSTANCE.returnObject(result);
                close();
            } catch (final IOException e) {
                activeRequests.remove(result);
                ProcessResponseResultPool.INSTANCE.returnObject(result);
                throw new RuntimeException(e);
            }
            return true;
        } else {
            return writing;
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
                        writeQueue.add(pollingResult);
                    }
                    pollingQueue.remove(pollingResult);
                }
                pollingResult = nextPollingResult;
            }
        }
    }

    @Override
    public boolean isHeartbeatTimeout() {
        return endpointSession.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private boolean isRequestTimeout() {
        return endpointSession.getRequestTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    @SuppressWarnings("unchecked")
    private void dispatchProcessResponse(final ProcessResponseResult result) throws IOException {
        final IServiceSynchronousCommand<IByteBufferProvider> request = requestReader.readMessage();
        try {
            final int serviceId = request.getService();
            if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
                return;
            }
            final SynchronousEndpointService service = parent.getService(serviceId);
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
            final ServerMethodInfo methodInfo = service.getMethodInfo(methodId);
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
                final Future<Object> future = methodInfo.invoke(endpointSession.getSessionId(), request,
                        result.getResponse());
                if (future != null) {
                    result.setFuture(future);
                    result.setDelayedWriteResponse(true);
                    pollingQueue.add(result);
                } else {
                    writeQueue.add(result);
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
                result.setFuture(workExecutor.submit(new ProcessResponseRunnable(methodInfo, result)));
            }
        } finally {
            requestReader.readFinished();
        }
    }

    private final class ProcessResponseRunnable implements Callable<Object> {
        private final ServerMethodInfo methodInfo;
        private final ProcessResponseResult result;

        private ProcessResponseRunnable(final ServerMethodInfo methodInfo, final ProcessResponseResult result) {
            this.methodInfo = methodInfo;
            this.result = result;
        }

        @Override
        public Object call() {
            try {
                final ISerializingServiceSynchronousCommand<Object> response = result.getResponse();
                if (isRequestTimeout()) {
                    response.setService(methodInfo.getService().getServiceId());
                    response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    response.setSequence(result.getRequestCopy().getSequence());
                    response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ, "request timeout ["
                            + endpointSession.getRequestTimeout() + "] exceeded, please try again later");
                    responseWriter.write(response);
                    return null;
                }
                final Future<Object> future = methodInfo.invoke(endpointSession.getSessionId(), result.getRequestCopy(),
                        response);
                if (future != null && !future.isDone()) {
                    result.setDelayedWriteResponse(true);
                    pollingQueueAsyncAdds.add(result);
                    return future;
                } else {
                    writeQueue.add(result);
                    return null;
                }
            } catch (final EOFException e) {
                close();
                return null;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}