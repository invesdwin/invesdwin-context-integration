package de.invesdwin.context.integration.channel.rpc.server.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.MutableServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.future.NullFuture;
import de.invesdwin.util.concurrent.lock.ILock;

@ThreadSafe
public class SynchronousEndpointServerSession implements Closeable {

    private static final WrappedExecutorService RESPONSE_EXECUTOR = Executors.newFixedThreadPool(
            SynchronousEndpointServerSession.class.getSimpleName() + "_RESPONSE", Executors.getCpuThreadPoolCount());

    private static final int MAX_PENDING_COUNT = 10_000;

    private final SynchronousEndpointServer parent;
    @GuardedBy("lock")
    private ISynchronousEndpointSession endpointSession;
    @GuardedBy("lock")
    private ISynchronousReader<IServiceSynchronousCommand<Object[]>> requestReader;
    @GuardedBy("lock")
    private final MutableServiceSynchronousCommand<Object> responseHolder = new MutableServiceSynchronousCommand<Object>();
    @GuardedBy("lock")
    private ISynchronousWriter<IServiceSynchronousCommand<Object>> responseWriter;
    @GuardedBy("lock")
    private long lastHeartbeatNanos = System.nanoTime();
    @GuardedBy("lock")
    private final int sequenceCounter = 0;
    private final ILock lock;
    private Future<?> processResponseFuture;

    public SynchronousEndpointServerSession(final SynchronousEndpointServer parent,
            final ISynchronousEndpointSession endpointSession) {
        this.parent = parent;
        this.lock = ILockCollectionFactory.getInstance(true)
                .newLock(SynchronousEndpointServerSession.class.getSimpleName() + "_lock");
        this.requestReader = endpointSession.newRequestReader(parent.getRequestSerde());
        this.responseWriter = endpointSession.newResponseWriter(parent.getResponseSerde());
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
        lock.lock();
        try {
            try {
                requestReader.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            requestReader = ClosedSynchronousReader.getInstance();
            try {
                responseWriter.close();
            } catch (final Throwable t) {
                Err.process(new RuntimeException("Ignoring", t));
            }
            responseWriter = ClosedSynchronousWriter.getInstance();
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

    //look for requests in clients, dispatch request handling and response sending to worker (handle heartbeat as well), return client for request monitoring after completion
    //reject executions if too many pending count for worker pool
    //check on start of worker task if timeout is already exceeded and abort directly (might have been in queue for too long)
    //maybe return exceptions to clients (similar to RmiExceptions that contain the stacktrace as message, full stacktrace in testing only?)
    //handle writeFinished in io thread (maybe the better idea?)
    //return true if work was done
    public boolean handle() throws IOException {
        if (processResponseFuture != null) {
            if (processResponseFuture.isDone()) {
                //keep flushing until finished and ready for next write
                if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                    processResponseFuture = null;
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
        if (requestReader.hasNext()) {
            lastHeartbeatNanos = System.nanoTime();
            final int pendingCount = RESPONSE_EXECUTOR.getPendingCount();
            if (pendingCount > MAX_PENDING_COUNT) {
                final IServiceSynchronousCommand<Object[]> request = requestReader.readMessage();
                try {
                    responseHolder.setService(request.getService());
                    responseHolder.setSequence(request.getSequence());
                } finally {
                    requestReader.readFinished();
                }
                responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                responseHolder.setMessage("too many requests pending [" + pendingCount + "], please try again later");
                responseWriter.write(responseHolder);
                processResponseFuture = NullFuture.getInstance();
            } else {
                processResponseFuture = RESPONSE_EXECUTOR.submit(this::processResponse);
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean isHeartbeatTimeout() {
        return endpointSession.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private boolean isRequestTimeout() {
        return endpointSession.getRequestTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private void processResponse() {
        try {
            final IServiceSynchronousCommand<Object[]> request = requestReader.readMessage();
            try {
                final SynchronousEndpointService service = parent.getService(request.getService());
                if (service == null) {
                    responseHolder.setService(request.getService());
                    responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
                    responseHolder.setSequence(request.getSequence());
                    responseHolder.setMessage("service not found: " + request.getService());
                    responseWriter.write(responseHolder);
                } else {
                    if (isRequestTimeout()) {
                        responseHolder.setService(request.getService());
                        responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
                        responseHolder.setSequence(request.getSequence());
                        responseHolder.setMessage("request timeout [" + endpointSession.getRequestTimeout()
                                + "] exceeded, please try again later");
                        responseWriter.write(responseHolder);
                    } else {
                        service.invoke(endpointSession.getSessionId(), request, responseHolder);
                        responseWriter.write(responseHolder);
                    }
                }
            } finally {
                requestReader.readFinished();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
