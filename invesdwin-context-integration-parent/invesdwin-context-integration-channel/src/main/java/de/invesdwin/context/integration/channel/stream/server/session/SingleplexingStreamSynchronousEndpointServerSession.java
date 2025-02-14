package de.invesdwin.context.integration.channel.stream.server.session;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.LazySerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.StreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.APostProcessingFuture;
import de.invesdwin.util.concurrent.future.Futures;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.error.FastNoSuchElementException;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * Allows only one active request per client session.
 */
@ThreadSafe
public class SingleplexingStreamSynchronousEndpointServerSession
        implements ISynchronousEndpointServerSession, IStreamSynchronousEndpointSession {

    private final StreamSynchronousEndpointServer parent;
    private ISynchronousEndpointSession endpointSession;
    private final String sessionId;
    private final Duration heartbeatTimeout;
    private final Duration requestTimeout;
    private ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> requestReader;
    private final LazySerializingServiceSynchronousCommand<Object> responseHolder = new LazySerializingServiceSynchronousCommand<Object>();
    private ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> responseWriter;
    private boolean delayedWriteResponse = false;
    @GuardedBy("volatile not needed because the same request runnable thread writes and reads this field only")
    private long lastHeartbeatNanos = System.nanoTime();
    private Future<Object> processResponseFuture;
    private ISynchronousReader<IByteBufferProvider> readFinishedReader;
    private final IStreamSessionManager manager;
    private int skipRequestReadingCount = 0;

    public SingleplexingStreamSynchronousEndpointServerSession(final StreamSynchronousEndpointServer parent,
            final ISynchronousEndpointSession endpointSession) {
        this.parent = parent;
        this.manager = parent.newManager(this);
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
    public StreamSynchronousEndpointServer getParent() {
        return parent;
    }

    @Override
    public void close() {
        final Future<?> processResponseFutureCopy = processResponseFuture;
        if (processResponseFutureCopy != null) {
            processResponseFutureCopy.cancel(true);
            processResponseFuture = null;
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
        if (processResponseFuture != null) {
            //a request processing is still active which should be handled by the handleRequests method
            throw FastNoSuchElementException.getInstance("processResponseFuture is not null");
        }
        final IByteBufferProvider message = reader.readMessage();
        responseHolder.setService(service.getServiceId());
        responseHolder.setMethod(StreamServerMethodInfo.METHOD_ID_PUSH);
        responseHolder.setSequence(-1);
        responseHolder.setMessageBuffer(message);
        responseWriter.write(responseHolder);
        final boolean flushed = responseWriter.writeFlushed();
        if (flushed) {
            responseHolder.close();
            reader.readFinished();
        } else {
            //let handleRequests flush the message
            processResponseFuture = NullFuture.getInstance();
            readFinishedReader = reader;
        }
        return flushed;
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
                    skipRequestReadingCount = parent.getMaxSuccessivePushCountPerSession();
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

    //look for requests in clients, dispatch request handling and response sending to worker (handle heartbeat as well), return client for request monitoring after completion
    //reject executions if too many pending count for worker pool
    //check on start of worker task if timeout is already exceeded and abort directly (might have been in queue for too long)
    //maybe return exceptions to clients (similar to RmiExceptions that contain the stacktrace as message, full stacktrace in testing only?)
    //handle writeFinished in io thread (maybe the better idea?)
    //return true if work was done
    private boolean handleRequests() throws IOException {
        if (processResponseFuture != null) {
            if (isProcessResponseFutureDone()) {
                if (delayedWriteResponse) {
                    responseWriter.write(responseHolder);
                    delayedWriteResponse = false;
                }
                //keep flushing until finished and ready for next write
                if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                    responseHolder.close(); //free memory
                    processResponseFuture = null;
                    if (readFinishedReader != null) {
                        readFinishedReader.readFinished();
                        readFinishedReader = null;
                    }
                    //directly check for next request
                } else {
                    //tell we are busy with writing or next write is not ready
                    return true;
                }
            } else {
                //throttle while waiting for response processing to finish
                return false;
            }
        }
        if (skipRequestReadingCount > 0) {
            return false;
        }
        if (requestReader.hasNext()) {
            lastHeartbeatNanos = System.nanoTime();
            dispatchProcessResponse();
            return true;
        } else {
            return false;
        }
    }

    private boolean isProcessResponseFutureDone() {
        if (processResponseFuture.isDone()) {
            if (processResponseFuture.isCancelled()) {
                delayedWriteResponse = false;
                return true;
            }
            final Future<?> future = (Future<?>) Futures.getNoInterrupt(processResponseFuture);
            if (future != null) {
                return future.isDone();
            } else {
                return true;
            }
        } else {
            return false;
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
        if (endpointSession == null) {
            return true;
        }
        return heartbeatTimeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private boolean isRequestTimeout() {
        if (endpointSession == null) {
            return true;
        }
        return requestTimeout.isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private void dispatchProcessResponse() throws IOException {
        final IServiceSynchronousCommand<IByteBufferProvider> request = requestReader.readMessage();
        final int serviceId = request.getService();
        if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return;
        }
        final int methodId = request.getMethod();
        final StreamServerMethodInfo methodInfo = StreamServerMethodInfo.valueOfNullable(methodId);
        if (methodInfo == null) {
            responseHolder.setService(serviceId);
            responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            responseHolder.setSequence(request.getSequence());
            responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "method not found: " + methodId);
            responseWriter.write(responseHolder);
            processResponseFuture = NullFuture.getInstance();
            return;
        }

        final WrappedExecutorService workExecutor = parent.getWorkExecutor();
        if (workExecutor == null || methodInfo.isBlocking()) {
            final Future<Object> future = processResponse(request, methodInfo);
            if (future != null) {
                delayedWriteResponse = true;
                processResponseFuture = future;
            } else {
                responseWriter.write(responseHolder);
                processResponseFuture = NullFuture.getInstance();
            }
        } else {
            final int maxPendingWorkCountOverall = parent.getMaxPendingWorkCountOverall();
            if (maxPendingWorkCountOverall > 0) {
                final int pendingCountOverall = workExecutor.getPendingCount();
                if (pendingCountOverall > maxPendingWorkCountOverall) {
                    try {
                        responseHolder.setService(serviceId);
                        responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                        responseHolder.setSequence(request.getSequence());
                        responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                                "too many requests pending overall [" + pendingCountOverall
                                        + "], please try again later");
                        responseWriter.write(responseHolder);
                        processResponseFuture = NullFuture.getInstance();
                        return;
                    } finally {
                        requestReader.readFinished();
                    }
                }
            }
            processResponseFuture = workExecutor.submit(() -> {
                try {
                    final Future<Object> future = processResponse(request, methodInfo);
                    if (future != null && !future.isDone()) {
                        return new APostProcessingFuture<Object>(future) {
                            @Override
                            protected Object onSuccess(final Object value) throws ExecutionException {
                                try {
                                    responseWriter.write(responseHolder);
                                } catch (final EOFException e) {
                                    close();
                                } catch (final IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return null;
                            }

                            @Override
                            protected ExecutionException onError(final ExecutionException exc) {
                                throw new UnsupportedOperationException(
                                        "should not be invoked here because exceptions should be handled by the service post processing",
                                        exc);
                            }
                        };
                    } else {
                        responseWriter.write(responseHolder);
                        return null;
                    }
                } catch (final IOException e) {
                    close();
                    return null;
                }
            });
        }
    }

    private Future<Object> processResponse(final IServiceSynchronousCommand<IByteBufferProvider> request,
            final StreamServerMethodInfo methodInfo) {
        try {
            try {
                if (isClosed()) {
                    return null;
                } else if (isRequestTimeout()) {
                    responseHolder.setService(request.getSequence());
                    responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    responseHolder.setSequence(request.getSequence());
                    responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                            "request timeout [" + requestTimeout + "] exceeded, please try again later");
                    responseWriter.write(responseHolder);
                    return null;
                }

                return methodInfo.invoke(manager, sessionId, request, responseHolder);
            } finally {
                requestReader.readFinished();
            }
        } catch (final EOFException e) {
            close();
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
