package de.invesdwin.context.integration.channel.rpc.client.session.multi;

import java.util.concurrent.ScheduledFuture;
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
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.lock.ILock;
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
    private ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> requestWriter;
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
    private final ManyToOneConcurrentLinkedQueue<MultiplexingSynchronousEndpointClientSessionResponse> addPollingRequests = new ManyToOneConcurrentLinkedQueue<>();
    @GuardedBy("only the activePolling thread should have access to this map")
    private final Int2ObjectOpenHashMap<MultiplexingSynchronousEndpointClientSessionResponse> pollingRequests = new Int2ObjectOpenHashMap<>();

    public MultiplexingSynchronousEndpointClientSession(final ISynchronousEndpointSession endpointSession) {
        this.endpointSession = endpointSession;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(MultiplexingSynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriter = endpointSession.newRequestWriter(ByteBufferProviderSerde.GET);
        this.responseReader = endpointSession.newResponseReader(ByteBufferProviderSerde.GET);
        try {
            requestWriter.open();
            responseReader.open();
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
        this.heartbeatFuture = HEARTBEAT_EXECUTOR.scheduleWithFixedDelay(this::maybeSendHeartbeat,
                endpointSession.getHeartbeatInterval().longValue(), endpointSession.getHeartbeatInterval().longValue(),
                endpointSession.getHeartbeatInterval().getTimeUnit().timeUnitValue());
    }

    public void maybeSendHeartbeat() {
        if (lock.tryLock()) {
            try {
                if (endpointSession.getHeartbeatInterval().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos)) {
                    try {
                        writeLocked(IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID, -1, -1, EmptyByteBuffer.INSTANCE,
                                System.nanoTime());
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            //no need to interrupt because we have the lock
            heartbeatFuture.cancel(false);
            try {
                requestWriter.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            requestWriter = ClosedSynchronousWriter.getInstance();
            try {
                responseReader.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            responseReader = ClosedSynchronousReader.getInstance();
            if (endpointSession != null) {
                try {
                    endpointSession.close();
                } catch (final Throwable t) {
                    Err.process(new RuntimeException("Ignoring", t));
                }
                endpointSession = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ICloseableByteBufferProvider request(final ClientMethodInfo methodInfo, final IByteBufferProvider request) {
        final MultiplexingSynchronousEndpointClientSessionResponse response = MultiplexingSynchronousEndpointClientSessionResponsePool.INSTANCE
                .borrowObject();
        final int requestSequence = sequenceCounter.incrementAndGet();
        response.init(methodInfo, request, requestSequence, activePolling);
        if (activePolling.compareAndSet(false, true)) {
            //take over the job of the activePolling thread and handle other requests while polling
            try {
                return requestActivePolling(response);
            } finally {
                activePolling.set(false);
            }
        } else {
            addPollingRequests.add(response);
            try {
                while (true) {
                    if (response.getCompletedSpinWait()
                            .awaitFulfill(System.nanoTime(), endpointSession.getRequestWaitInterval())) {
                        if (response.isCompleted()) {
                            //the other thread finished our work for us
                            return response;
                        } else if (activePolling.compareAndSet(false, true)) {
                            //take over the job of the other activePolling thread that just go finished
                            try {
                                return requestActivePolling(response);
                            } finally {
                                activePolling.set(false);
                            }
                        }
                    }
                }
            } catch (final Throwable t) {
                throw new RetryLaterRuntimeException(t);
            }
        }
    }

    private MultiplexingSynchronousEndpointClientSessionResponse requestActivePolling(
            final MultiplexingSynchronousEndpointClientSessionResponse response) {
        lock.lock();
        try {
            final long waitingSinceNanos = System.nanoTime();
            final ClientMethodInfo methodInfo = response.getMethodInfo();
            final int requestSequence = response.getRequestSequence();
            if (response.getRequest() != null) {
                writeLocked(methodInfo.getServiceId(), methodInfo.getMethodId(), requestSequence, response.getRequest(),
                        waitingSinceNanos);
                response.requestWritten();
            }
            while (true) {
                //                while (!responseReader.hasNext()
                //                        .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                //                    if (endpointSession.getRequestTimeout()
                //                            .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                //                        throw new TimeoutException("Request timeout exceeded for [" + methodInfo.getServiceId() + ":"
                //                                + methodInfo.getMethodId() + "]: " + endpointSession.getRequestTimeout());
                //                    }
                //                }
                try (IServiceSynchronousCommand<IByteBufferProvider> responseHolder = responseReader.readMessage()) {
                    final int responseService = responseHolder.getService();
                    final int responseMethod = responseHolder.getMethod();
                    final int responseSequence = responseHolder.getSequence();
                    if (responseSequence != requestSequence) {
                        //ignore invalid response and wait for correct one (might happen due to previous timeout and late response)
                        continue;
                    }
                    if (responseService != methodInfo.getServiceId()) {
                        throw new RetryLaterRuntimeException("Unexpected serviceId in response [" + responseService
                                + "] for request [" + methodInfo.getServiceId() + "]");
                    }
                    lastHeartbeatNanos = System.nanoTime();
                    if (responseMethod != methodInfo.getMethodId()) {
                        if (responseMethod == IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID) {
                            final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                            final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                            throw new RetryLaterRuntimeException(new RemoteExecutionException(message));
                        } else if (responseMethod == IServiceSynchronousCommand.ERROR_METHOD_ID) {
                            final IByteBuffer messageBuffer = responseHolder.getMessage().asBuffer();
                            final String message = messageBuffer.getStringUtf8(0, messageBuffer.capacity());
                            throw new RemoteExecutionException(message);
                        } else {
                            throw new RetryLaterRuntimeException("Unexpected methodId in response [" + +responseMethod
                                    + "] for request [" + methodInfo.getMethodId() + "]");
                        }
                    }
                    final IByteBufferProvider responseMessage = responseHolder.getMessage();
                    response.responseCompleted(responseMessage);
                    return response;
                } catch (final Throwable e) {
                    responseReader.readFinished();
                    Throwables.propagate(e);
                }
            }
        } catch (final Throwable e) {
            lock.unlock();
            throw new RetryLaterRuntimeException(e);
        }
    }

    private void writeLocked(final int serviceId, final int methodId, final int requestSequence,
            final IByteBufferProvider request, final long waitingSinceNanos) throws Exception {
        //        try {
        //            requestHolder.setService(serviceId);
        //            requestHolder.setMethod(methodId);
        //            requestHolder.setSequence(requestSequence);
        //            requestHolder.setMessage(request);
        //            while (!requestWriter.writeReady()
        //                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
        //                if (endpointSession.getRequestTimeout()
        //                        .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
        //                    throw new TimeoutException("Request write ready timeout exceeded for [" + serviceId + ":" + methodId
        //                            + "]: " + endpointSession.getRequestTimeout());
        //                }
        //            }
        //            requestWriter.getWriter().write(requestHolder);
        //            while (!requestWriter.writeFlushed()
        //                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
        //                if (endpointSession.getRequestTimeout()
        //                        .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
        //                    throw new TimeoutException("Request write flush timeout exceeded for [" + serviceId + ":" + methodId
        //                            + "]: " + endpointSession.getRequestTimeout());
        //                }
        //            }
        //        } finally {
        //            requestHolder.close(); //free memory
        //        }
    }

}
