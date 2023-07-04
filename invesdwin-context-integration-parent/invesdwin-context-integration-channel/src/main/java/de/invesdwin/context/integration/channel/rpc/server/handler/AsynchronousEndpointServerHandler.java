package de.invesdwin.context.integration.channel.rpc.server.handler;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.handler.poll.IPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService.ServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing.LazyDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.LazySerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AsynchronousEndpointServerHandler
        implements IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider>, IByteBufferProvider {

    private final AsynchronousEndpointServerHandlerFactory parent;
    private final LazyDeserializingServiceSynchronousCommand<IByteBufferProvider> requestHolder = new LazyDeserializingServiceSynchronousCommand<>();
    private final LazySerializingServiceSynchronousCommand<Object> responseHolder = new LazySerializingServiceSynchronousCommand<Object>();
    private final ServiceSynchronousCommandSerde<IByteBufferProvider> outputSerde = new ServiceSynchronousCommandSerde<>(
            ByteBufferProviderSerde.GET, null);
    private volatile IServiceSynchronousCommand<IByteBufferProvider> output;
    private IByteBuffer outputBuffer;
    private long lastHeartbeatNanos = System.nanoTime();
    private final IPollingQueueProvider pollingQueueProvider;

    public AsynchronousEndpointServerHandler(final AsynchronousEndpointServerHandlerFactory parent) {
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
        responseHolder.close();
        output = null;
        outputBuffer = null;
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

    private boolean isRequestTimeout() {
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
            output = dispatchProcessResponse(context);
            if (output != null) {
                return this;
            } else {
                return null;
            }
        } finally {
            requestHolder.close();
        }
    }

    @Override
    public void outputFinished(final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        output = null;
        responseHolder.close();
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        return outputSerde.toBuffer(dst, output);
    }

    @Override
    public IByteBuffer asBuffer() {
        if (outputBuffer == null) {
            //needs to be expandable so that FragmentSynchronousWriter can work properly
            outputBuffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(outputBuffer);
        return outputBuffer.slice(0, length);
    }

    private IServiceSynchronousCommand<IByteBufferProvider> dispatchProcessResponse(
            final IAsynchronousHandlerContext<IByteBufferProvider> context) throws IOException {
        final int serviceId = requestHolder.getService();
        if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return null;
        }
        final SynchronousEndpointService service = parent.getService(serviceId);
        if (service == null) {
            responseHolder.setService(serviceId);
            responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            responseHolder.setSequence(requestHolder.getSequence());
            responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "service not found: " + serviceId);
            return responseHolder;
        }
        final int methodId = requestHolder.getMethod();
        final ServerMethodInfo methodInfo = service.getMethodInfo(methodId);
        if (methodInfo == null) {
            responseHolder.setService(serviceId);
            responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            responseHolder.setSequence(requestHolder.getSequence());
            responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "method not found: " + methodId);
            return responseHolder;
        }

        final ProcessResponseResult result = context.borrowResult();
        result.setContext(context);
        final WrappedExecutorService workExecutor = parent.getWorkExecutor();
        if (workExecutor == null || methodInfo.isBlocking()) {
            final Future<Object> future = methodInfo.invoke(context.getSessionId(), requestHolder,
                    result.getResponse());
            if (future != null && !future.isDone()) {
                result.setFuture(future);
                result.setDelayedWriteResponse(true);
                pollingQueueProvider.addToPollingQueue(result);
                return null;
            } else {
                return result.getResponse();
            }
        } else {
            final int maxPendingWorkCountPerSession = parent.getMaxPendingWorkCountPerSession();
            if (maxPendingWorkCountPerSession > 0) {
                final int thisPendingCount = workExecutor.getPendingCount();
                if (thisPendingCount > maxPendingWorkCountPerSession) {
                    responseHolder.setService(serviceId);
                    responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    responseHolder.setSequence(requestHolder.getSequence());
                    responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                            "too many requests pending for this session [" + thisPendingCount
                                    + "], please try again later");
                    return responseHolder;
                }
            }
            final int maxPendingWorkCountOverall = parent.getMaxPendingWorkCountOverall();
            if (maxPendingWorkCountOverall > 0) {
                final int overallPendingCount = workExecutor.getPendingCount();
                if (overallPendingCount > maxPendingWorkCountOverall) {
                    responseHolder.setService(serviceId);
                    responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                    responseHolder.setSequence(requestHolder.getSequence());
                    responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                            "too many requests pending overall [" + overallPendingCount + "], please try again later");
                    return responseHolder;
                }
            }
            //copy request for the async processing
            result.getRequestCopy().copy(requestHolder);
            result.setFuture(workExecutor.submit(new ProcessResponseTask(methodInfo, result)));
            return null;
        }
    }

    private final class ProcessResponseTask implements Callable<Object> {
        private final ServerMethodInfo methodInfo;
        private final ProcessResponseResult result;

        private ProcessResponseTask(final ServerMethodInfo methodInfo, final ProcessResponseResult result) {
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
                result.getContext().write(response.asBuffer());
                result.close();
                return null;
            }
        }
    }

}
