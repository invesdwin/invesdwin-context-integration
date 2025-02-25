package de.invesdwin.context.integration.channel.rpc.base.server.async;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.IPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.base.server.service.RpcSynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.deserializing.LazyDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class RpcAsynchronousEndpointServerHandler
        implements IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> {

    private final RpcAsynchronousEndpointServerHandlerFactory parent;
    private final LazyDeserializingServiceSynchronousCommand<IByteBufferProvider> requestHolder = new LazyDeserializingServiceSynchronousCommand<>();
    private long lastHeartbeatNanos = System.nanoTime();
    private final IPollingQueueProvider pollingQueueProvider;
    private volatile boolean closed;

    public RpcAsynchronousEndpointServerHandler(final RpcAsynchronousEndpointServerHandlerFactory parent) {
        this.parent = parent;
        this.pollingQueueProvider = parent.getPollingQueueProvider();
    }

    @Override
    public IByteBufferProvider open(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        //noop
        return null;
    }

    @Override
    public void close() {
        requestHolder.close();
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public IByteBufferProvider idle(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        if (isHeartbeatTimeout()) {
            throw FastEOFException.getInstance("heartbeat timeout [%s] exceeded", parent.getHeartbeatTimeout());
        }
        return null;
    }

    public boolean isHeartbeatTimeout() {
        return parent.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    /**
     * This is not measured based on individual requests, instead it is measured based on the handler/session being
     * still active.
     */
    private boolean isRequestTimeout() {
        if (isClosed()) {
            return true;
        }
        return parent.getRequestTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    @Override
    public IByteBufferProvider handle(final IAsynchronousHandlerContext<IByteBufferProvider> context,
            final IByteBufferProvider input) throws IOException {
        lastHeartbeatNanos = System.nanoTime();
        final IByteBuffer buffer = input.asBuffer();
        final int service = buffer.getInt(ServiceSynchronousCommandSerde.SERVICE_INDEX);
        final int method = buffer.getInt(ServiceSynchronousCommandSerde.METHOD_INDEX);
        final int sequence = buffer.getInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - ServiceSynchronousCommandSerde.MESSAGE_INDEX;
        try {
            requestHolder.setService(service);
            requestHolder.setMethod(method);
            requestHolder.setSequence(sequence);
            requestHolder.setMessage(ByteBufferProviderSerde.GET,
                    buffer.slice(ServiceSynchronousCommandSerde.MESSAGE_INDEX, messageLength));
            //we expect async handlers to already be running inside of a worker pool, thus just execute in current thread
            return dispatchProcessResponse(context);
        } finally {
            requestHolder.close();
        }
    }

    private IByteBufferProvider dispatchProcessResponse(final IAsynchronousHandlerContext<IByteBufferProvider> context)
            throws IOException {
        final int serviceId = requestHolder.getService();
        if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return null;
        }
        final ProcessResponseResult result = context.borrowResult();
        result.setContext(context);
        final RpcSynchronousEndpointService service = parent.getService(serviceId);
        final EagerSerializingServiceSynchronousCommand<Object> response = result.getResponse();
        if (service == null) {
            response.setService(serviceId);
            response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            response.setSequence(requestHolder.getSequence());
            response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ, "service not found: " + serviceId);
            return response.asBuffer();
        }
        final int methodId = requestHolder.getMethod();
        final RpcServerMethodInfo methodInfo = service.getMethodInfo(methodId);
        if (methodInfo == null) {
            response.setService(serviceId);
            response.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            response.setSequence(requestHolder.getSequence());
            response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ, "method not found: " + methodId);
            return response.asBuffer();
        }

        final WrappedExecutorService workExecutor = parent.getWorkExecutor();
        if (workExecutor == null || methodInfo.isBlocking()) {
            final Future<Object> future = methodInfo.invoke(context.getSessionId(), requestHolder, response);
            if (future != null && !future.isDone()) {
                result.setFuture(future);
                result.setDelayedWriteResponse(true);
                pollingQueueProvider.addToPollingQueue(result);
                return null;
            } else {
                if (response.hasMessage()) {
                    return response.asBuffer();
                } else {
                    return null;
                }
            }
        } else {
            final int maxPendingWorkCountPerSession = parent.getMaxPendingWorkCountPerSession();
            if (maxPendingWorkCountPerSession > 0) {
                final int thisPendingCount = workExecutor.getPendingCount();
                if (thisPendingCount > maxPendingWorkCountPerSession) {
                    response.setService(serviceId);
                    response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    response.setSequence(requestHolder.getSequence());
                    response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                            "too many requests pending for this session [" + thisPendingCount
                                    + "], please try again later");
                    return response.asBuffer();
                }
            }
            final int maxPendingWorkCountOverall = parent.getMaxPendingWorkCountOverall();
            if (maxPendingWorkCountOverall > 0) {
                final int overallPendingCount = workExecutor.getPendingCount();
                if (overallPendingCount > maxPendingWorkCountOverall) {
                    response.setService(serviceId);
                    response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    response.setSequence(requestHolder.getSequence());
                    response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                            "too many requests pending overall [" + overallPendingCount + "], please try again later");
                    return response.asBuffer();
                }
            }
            //copy request for the async processing
            result.getRequestCopy().copy(requestHolder);
            result.setFuture(workExecutor.submit(new ProcessResponseTask(methodInfo, result)));
            return null;
        }
    }

    @Override
    public void outputFinished(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        //noop
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
            final EagerSerializingServiceSynchronousCommand<Object> response = result.getResponse();
            if (isRequestTimeout()) {
                response.setService(methodInfo.getService().getServiceId());
                response.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                response.setSequence(result.getRequestCopy().getSequence());
                response.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                        "request timeout [" + parent.getRequestTimeout() + "] exceeded, please try again later");
                result.getContext().write(response.asBuffer());
                result.close();
                return null;
            }
            final Future<Object> future = methodInfo.invoke(result.getContext().getSessionId(), result.getRequestCopy(),
                    response);
            if (future != null && !future.isDone()) {
                result.setDelayedWriteResponse(true);
                pollingQueueProvider.addToPollingQueue(result);
                return future;
            } else {
                if (response.hasMessage()) {
                    result.getContext().write(response.asBuffer());
                }
                result.close();
                return null;
            }
        }
    }

}
