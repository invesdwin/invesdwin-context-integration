package de.invesdwin.context.integration.channel.rpc.client.session.single;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.client.SynchronousEndpointClient.ClientMethodInfo;
import de.invesdwin.context.integration.channel.rpc.client.session.ISynchronousEndpointClientSession;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

@ThreadSafe
public class SingleplexingSynchronousEndpointClientSession implements ISynchronousEndpointClientSession {

    private static final WrappedScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(
            SingleplexingSynchronousEndpointClientSession.class.getSimpleName() + "_HEARTBEAT", 1);

    @GuardedBy("lock")
    private ISynchronousEndpointSession endpointSession;
    @GuardedBy("lock")
    private SynchronousWriterSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> requestWriterSpinWait;
    @GuardedBy("lock")
    private final MutableServiceSynchronousCommand<IByteBufferProvider> requestHolder = new MutableServiceSynchronousCommand<IByteBufferProvider>();
    @GuardedBy("lock")
    private SynchronousReaderSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> responseReaderSpinWait;
    @GuardedBy("lock")
    private long lastHeartbeatNanos = System.nanoTime();
    @GuardedBy("lock")
    private int sequenceCounter = 0;
    private final ILock lock;
    @GuardedBy("lock")
    private final ScheduledFuture<?> heartbeatFuture;

    private final SingleplexingSynchronousEndpointClientSessionResponse response;

    public SingleplexingSynchronousEndpointClientSession(final IObjectPool<ISynchronousEndpointClientSession> pool,
            final ISynchronousEndpointSession endpointSession) {
        this.endpointSession = endpointSession;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(SingleplexingSynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriterSpinWait = new SynchronousWriterSpinWait<>(
                endpointSession.newRequestWriter(ByteBufferProviderSerde.GET));
        this.responseReaderSpinWait = new SynchronousReaderSpinWait<>(
                endpointSession.newResponseReader(ByteBufferProviderSerde.GET));
        try {
            requestWriterSpinWait.getWriter().open();
            responseReaderSpinWait.getReader().open();
        } catch (final Throwable t) {
            close();
            throw new RuntimeException(t);
        }
        maybeSendHeartbeat();
        this.heartbeatFuture = EXECUTOR.scheduleWithFixedDelay(this::maybeSendHeartbeat,
                endpointSession.getHeartbeatInterval().longValue(), endpointSession.getHeartbeatInterval().longValue(),
                endpointSession.getHeartbeatInterval().getTimeUnit().timeUnitValue());

        this.response = new SingleplexingSynchronousEndpointClientSessionResponse(pool, this, lock,
                responseReaderSpinWait.getReader());
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
                requestWriterSpinWait.getWriter().close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            requestWriterSpinWait = ClosedSynchronousWriter.getSpinWait();
            try {
                responseReaderSpinWait.getReader().close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            responseReaderSpinWait = ClosedSynchronousReader.getSpinWait();
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
        lock.lock();
        try {
            final int requestSequence = sequenceCounter++;
            final long waitingSinceNanos = System.nanoTime();
            writeLocked(methodInfo.getServiceId(), methodInfo.getMethodId(), requestSequence, request,
                    waitingSinceNanos);
            while (true) {
                while (!responseReaderSpinWait.hasNext()
                        .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                    if (endpointSession.getRequestTimeout()
                            .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                        throw new TimeoutException("Request timeout exceeded for [" + methodInfo.getServiceId() + ":"
                                + methodInfo.getMethodId() + "]: " + endpointSession.getRequestTimeout());
                    }
                }
                try (IServiceSynchronousCommand<IByteBufferProvider> responseHolder = responseReaderSpinWait.getReader()
                        .readMessage()) {
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
                    response.setMessage(responseMessage);
                    return response;
                } catch (final Throwable e) {
                    responseReaderSpinWait.getReader().readFinished();
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
        try {
            requestHolder.setService(serviceId);
            requestHolder.setMethod(methodId);
            requestHolder.setSequence(requestSequence);
            requestHolder.setMessage(request);
            while (!requestWriterSpinWait.writeReady()
                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                if (endpointSession.getRequestTimeout()
                        .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                    throw new TimeoutException("Request write ready timeout exceeded for [" + serviceId + ":" + methodId
                            + "]: " + endpointSession.getRequestTimeout());
                }
            }
            requestWriterSpinWait.getWriter().write(requestHolder);
            while (!requestWriterSpinWait.writeFlushed()
                    .awaitFulfill(waitingSinceNanos, endpointSession.getRequestWaitInterval())) {
                if (endpointSession.getRequestTimeout()
                        .isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                    throw new TimeoutException("Request write flush timeout exceeded for [" + serviceId + ":" + methodId
                            + "]: " + endpointSession.getRequestTimeout());
                }
            }
        } finally {
            requestHolder.close(); //free memory
        }
    }

}