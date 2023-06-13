package de.invesdwin.context.integration.channel.rpc.server.async;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService.ServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing.LazyDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.LazySerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResultPool;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class AsynchronousEndpointServerHandler
        implements IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider>, IByteBufferProvider {

    private static final ManyToOneConcurrentLinkedQueue<ProcessResponseResult> POLLING_QUEUE_ADDS = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("POLLING_QUEUE_ADDS")
    private static WrappedExecutorService pollingExecutor;

    private final AsynchronousEndpointServerHandlerFactory parent;
    private final LazyDeserializingServiceSynchronousCommand<IByteBufferProvider> requestHolder = new LazyDeserializingServiceSynchronousCommand<>();
    private final LazySerializingServiceSynchronousCommand<Object> responseHolder = new LazySerializingServiceSynchronousCommand<Object>();
    private final ServiceSynchronousCommandSerde<IByteBufferProvider> outputSerde = new ServiceSynchronousCommandSerde<>(
            ByteBufferProviderSerde.GET, null);
    private volatile IServiceSynchronousCommand<IByteBufferProvider> output;
    private IByteBuffer outputBuffer;
    private long lastHeartbeatNanos = System.nanoTime();

    public AsynchronousEndpointServerHandler(final AsynchronousEndpointServerHandlerFactory parent) {
        this.parent = parent;
    }

    private static void addToPollingQueue(final ProcessResponseResult result) {
        synchronized (POLLING_QUEUE_ADDS) {
            if (pollingExecutor == null) {
                //reduce cpu load by using max 1 thread
                pollingExecutor = Executors
                        .newFixedThreadPool(AsynchronousEndpointServerHandler.class.getSimpleName() + "_POLLING", 1)
                        .setDynamicThreadName(false);
                pollingExecutor.execute(new PollingQueueRunnable());
            }
            POLLING_QUEUE_ADDS.add(result);
        }
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

    @SuppressWarnings("unchecked")
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

        final ProcessResponseResult result = ProcessResponseResultPool.INSTANCE.borrowObject();
        final WrappedExecutorService workExecutor = parent.getWorkExecutor();
        if (workExecutor == null || methodInfo.isBlocking()) {
            final Future<Object> future = methodInfo.invoke(context.getSessionId(), requestHolder, responseHolder);
            if (future != null && !future.isDone()) {
                result.setFuture(future);
                result.setDelayedWriteResponse(true);
                result.setContext(context);
                addToPollingQueue(result);
                return null;
            } else {
                return responseHolder;
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
            result.setContext(context);
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
                ProcessResponseResultPool.INSTANCE.returnObject(result);
                return null;
            }
            final Future<Object> future = methodInfo.invoke(result.getContext().getSessionId(), result.getRequestCopy(),
                    response);
            if (future != null && !future.isDone()) {
                result.setDelayedWriteResponse(true);
                addToPollingQueue(result);
                return future;
            } else {
                result.getContext().write(response.asBuffer());
                ProcessResponseResultPool.INSTANCE.returnObject(result);
                return null;
            }
        }
    }

    private static final class PollingQueueRunnable implements Runnable {

        private final ASpinWait spinWait = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return maybePollResults();
            }
        };
        private long lastChangeNanos = System.nanoTime();
        private final NodeBufferingIterator<ProcessResponseResult> pollingQueue = new NodeBufferingIterator<>();

        @Override
        public void run() {
            try {
                while (true) {
                    if (!spinWait.awaitFulfill(System.nanoTime(), Duration.ONE_MINUTE)) {
                        if (maybeClosePollingExecutor()) {
                            return;
                        }
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        private boolean maybeClosePollingExecutor() {
            if (isTimeout()) {
                synchronized (POLLING_QUEUE_ADDS) {
                    if (isTimeout()) {
                        pollingExecutor.shutdown();
                        pollingExecutor = null;
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isTimeout() {
            return pollingQueue.isEmpty() && POLLING_QUEUE_ADDS.isEmpty()
                    && Duration.TEN_MINUTES.isLessThanNanos(System.nanoTime() - lastChangeNanos);
        }

        private boolean maybePollResults() {
            boolean changed = false;
            if (!POLLING_QUEUE_ADDS.isEmpty()) {
                ProcessResponseResult addPollingResult = POLLING_QUEUE_ADDS.poll();
                while (addPollingResult != null) {
                    pollingQueue.add(addPollingResult);
                    changed = true;
                    addPollingResult = POLLING_QUEUE_ADDS.poll();
                    lastChangeNanos = System.nanoTime();
                }
            }
            if (!pollingQueue.isEmpty()) {
                ProcessResponseResult pollingResult = pollingQueue.getHead();
                while (pollingResult != null) {
                    final ProcessResponseResult nextPollingResult = pollingResult.getNext();
                    if (pollingResult.isDone()) {
                        if (pollingResult.isDelayedWriteResponse()) {
                            pollingResult.getContext().write(pollingResult.getResponse().asBuffer());
                        }
                        pollingQueue.remove(pollingResult);
                        changed = true;
                        ProcessResponseResultPool.INSTANCE.returnObject(pollingResult);
                        lastChangeNanos = System.nanoTime();
                    }
                    pollingResult = nextPollingResult;
                }
            }
            return changed;
        }
    }

}
