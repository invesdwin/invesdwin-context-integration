package de.invesdwin.context.integration.channel.rpc.client.session;

import java.io.Closeable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.client.RemoteExecutionException;
import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.service.command.MutableServiceSynchronousCommand;
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
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.streams.buffer.bytes.EmptyByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class SynchronousEndpointClientSession implements Closeable {

    private static final WrappedScheduledExecutorService EXECUTOR = Executors
            .newScheduledThreadPool(SynchronousEndpointClientSession.class.getSimpleName() + "_HEARTBEAT", 1);

    @GuardedBy("lock")
    private ISynchronousEndpointSession info;
    @GuardedBy("lock")
    private SynchronousWriterSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> requestWriterSpinWait;
    @GuardedBy("lock")
    private final MutableServiceSynchronousCommand<IByteBufferProvider> holder = new MutableServiceSynchronousCommand<IByteBufferProvider>();
    @GuardedBy("lock")
    private SynchronousReaderSpinWait<IServiceSynchronousCommand<IByteBufferProvider>> responseReaderSpinWait;
    @GuardedBy("lock")
    private long lastHeartbeatNanos = System.nanoTime();
    @GuardedBy("lock")
    private int sequenceCounter = 0;
    private final ILock lock;
    @GuardedBy("lock")
    private final ScheduledFuture<?> heartbeatFuture;

    private final SynchronousEndpointClientSessionResponse response;

    public SynchronousEndpointClientSession(final SynchronousEndpointClientSessionPool pool,
            final ISynchronousEndpointSession endpointSession) {
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(SynchronousEndpointClientSession.class.getSimpleName() + "_lock");
        this.requestWriterSpinWait = new SynchronousWriterSpinWait<>(endpointSession.newRequestWriter());
        this.responseReaderSpinWait = new SynchronousReaderSpinWait<>(endpointSession.newResponseReader());
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

        this.response = new SynchronousEndpointClientSessionResponse(pool, this, lock,
                responseReaderSpinWait.getReader());
    }

    public void maybeSendHeartbeat() {
        if (lock.tryLock()) {
            try {
                if (info.getHeartbeatInterval().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos)) {
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
            if (info != null) {
                try {
                    info.close();
                } catch (final Throwable t) {
                    Err.process(new RuntimeException("Ignoring", t));
                }
                info = null;
            }
        } finally {
            lock.unlock();
        }
    }

    public SynchronousEndpointClientSessionResponse request(final int requestService, final int requestMethod,
            final IByteBufferProvider request) {
        lock.lock();
        try {
            final int requestSequence = sequenceCounter++;
            final long waitingSinceNanos = System.nanoTime();
            writeLocked(requestService, requestMethod, requestSequence, request, waitingSinceNanos);
            while (true) {
                while (!responseReaderSpinWait.hasNext()
                        .awaitFulfill(waitingSinceNanos, info.getRequestWaitInterval())) {
                    if (info.getRequestTimeout().isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                        throw new TimeoutException("Request timeout exceeded for [" + requestService + ":"
                                + requestMethod + "]: " + info.getRequestTimeout());
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
                    if (responseService != requestService) {
                        throw new RetryLaterRuntimeException("Unexpected serviceId in response [" + responseService
                                + "] for request [" + requestService + "]");
                    }
                    lastHeartbeatNanos = System.nanoTime();
                    if (responseMethod != requestMethod) {
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
                                    + "] for request [" + requestMethod + "]");
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

    private void writeLocked(final int requestService, final int requestMethod, final int requestSequence,
            final IByteBufferProvider request, final long waitingSinceNanos) throws Exception {
        holder.setService(requestService);
        holder.setMethod(requestMethod);
        holder.setSequence(requestSequence);
        holder.setMessage(request);
        while (!requestWriterSpinWait.writeReady().awaitFulfill(waitingSinceNanos, info.getRequestWaitInterval())) {
            if (info.getRequestTimeout().isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                throw new TimeoutException("Request write ready timeout exceeded for [" + requestService + ":"
                        + requestMethod + "]: " + info.getRequestTimeout());
            }
        }
        requestWriterSpinWait.getWriter().write(holder);
        while (!requestWriterSpinWait.writeFlushed().awaitFulfill(waitingSinceNanos, info.getRequestWaitInterval())) {
            if (info.getRequestTimeout().isLessThanOrEqualToNanos(System.nanoTime() - waitingSinceNanos)) {
                throw new TimeoutException("Request write flush timeout exceeded for [" + requestService + ":"
                        + requestMethod + "]: " + info.getRequestTimeout());
            }
        }
        holder.setMessage(null); //free memory
    }

    /**
     * Needs to be closed to finish the reuquest and return objects back to the pool
     */
    public SynchronousEndpointClientSessionResponse getResponse() {
        return response;
    }

}
