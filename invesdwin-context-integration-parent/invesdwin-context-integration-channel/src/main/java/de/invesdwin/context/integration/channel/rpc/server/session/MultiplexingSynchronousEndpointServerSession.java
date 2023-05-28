package de.invesdwin.context.integration.channel.rpc.server.session;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.command.CopyBufferServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.ISerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousReader;
import de.invesdwin.context.integration.channel.sync.ClosedSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.collections.fast.IFastIterableSet;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator.INode;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.pool.AAgronaObjectPool;
import de.invesdwin.util.concurrent.pool.AQueueObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * Allows to process multiple requests in parallel for the same endpoint by multiplexing it.
 */
@ThreadSafe
public class MultiplexingSynchronousEndpointServerSession implements ISynchronousEndpointServerSession {

    private static final ProcessResponseResult[] PROCESS_RESPONSE_RESULT_EMPTY_ARRAY = new ProcessResponseResult[0];

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
    private final IFastIterableSet<ProcessResponseResult> activeRequests = ILockCollectionFactory.getInstance(false)
            .newFastIterableIdentitySet();
    @GuardedBy("only the io thread should access the result pool")
    private final IObjectPool<ProcessResponseResult> resultPool = new AQueueObjectPool<ProcessResponseResult>(
            new OneToOneConcurrentArrayQueue<>(AAgronaObjectPool.DEFAULT_MAX_POOL_SIZE)) {
        @Override
        protected ProcessResponseResult newObject() {
            return new ProcessResponseResult();
        }

        @Override
        protected boolean passivateObject(final ProcessResponseResult element) {
            element.close();
            return true;
        }
    };
    private final WrappedExecutorService responseExecutor;

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
        this.responseExecutor = parent.getWorkExecutor();
        if (responseExecutor == null) {
            throw new NullPointerException("responseExecutor should not be null");
        }
    }

    protected ISerializingServiceSynchronousCommand<Object> newResponseHolder() {
        //ensures that the worker thread does the serialization at the cost of an additional copy (useful when serialization/marshalling can be expensive)
        return new EagerSerializingServiceSynchronousCommand<Object>();
        //lets the IO thread do the serialization, might not be a good idea unless this guaranteed to be fast
        //        return new LazySerializingServiceSynchronousCommand<Object>();
    }

    @Override
    public ISynchronousEndpointSession getEndpointSession() {
        return endpointSession;
    }

    @Override
    public void close() {
        //only the IO thread will access the active requests array, also only IO thread will call close of the server session, thus we are fine here
        final ProcessResponseResult[] activeRequestsArray = activeRequests.asArray(PROCESS_RESPONSE_RESULT_EMPTY_ARRAY);
        for (int i = 0; i < activeRequestsArray.length; i++) {
            final ProcessResponseResult activeRequest = activeRequestsArray[i];
            final ListenableFuture<?> futureCopy = activeRequest.future;
            if (futureCopy != null) {
                activeRequest.future = null;
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
        final boolean writing;
        final ProcessResponseResult writeTask = writeQueue.peek();
        if (writeTask != null) {
            /*
             * even if we finish writing the current task and no other task follows, we still handled something and
             * continue eagerly looking for more work (client might immediately give us a new task)
             */
            writing = true;
            if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                if (writeTask.writing) {
                    final ProcessResponseResult removedTask = writeQueue.remove();
                    final ProcessResponseResult nextWriteTask = writeQueue.peek();
                    if (nextWriteTask != null) {
                        responseWriter.write(nextWriteTask.responseHolder);
                        nextWriteTask.writing = true;
                    }
                    activeRequests.remove(writeTask);
                    resultPool.returnObject(writeTask);
                    Assertions.checkSame(writeTask, removedTask);
                } else {
                    responseWriter.write(writeTask.responseHolder);
                    writeTask.writing = true;
                }
            }
        } else {
            //reading could still indicate that we are busy handling work
            writing = false;
        }
        if (requestReader.hasNext()) {
            lastHeartbeatNanos = System.nanoTime();

            final int thisPendingCount = activeRequests.size();
            if (thisPendingCount > parent.getMaxPendingWorkCountPerSession()) {
                rejectPendingCount("too many requests pending for this session [" + thisPendingCount
                        + "], please try again later");
            }
            final int overallPendingCount = responseExecutor.getPendingCount();
            if (overallPendingCount > parent.getMaxPendingWorkCountOverall()) {
                rejectPendingCount(
                        "too many requests pending overall [" + overallPendingCount + "], please try again later");
                return true;
            }
            final ProcessResponseResult result = resultPool.borrowObject();
            try {
                /*
                 * Keep actual request in read as shortly as possible so that the maybe blocking transport can send the
                 * message as soon as possible. Sadly we need to make a copy of the request buffer here, since we don't
                 * want to do an expensive deserialization of the args in the IO thread and because we want to read the
                 * next message while processing for other requests is still active.
                 */
                try (IServiceSynchronousCommand<IByteBufferProvider> request = requestReader.readMessage()) {
                    result.requestHolder.copy(request);
                } finally {
                    requestReader.readFinished();
                }
                final ListenableFuture<?> future = responseExecutor.submit(() -> processResponse(result));
                result.future = future;
                activeRequests.add(result);
            } catch (final EOFException e) {
                resultPool.returnObject(result);
                close();
            } catch (final IOException e) {
                resultPool.returnObject(result);
                throw new RuntimeException(e);
            }
            return true;
        } else {
            return writing;
        }
    }

    private void rejectPendingCount(final String message) throws IOException {
        final int serviceId;
        final int sequence;
        try (IServiceSynchronousCommand<IByteBufferProvider> request = requestReader.readMessage()) {
            serviceId = request.getService();
            if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
                return;
            }
            sequence = request.getSequence();
        } finally {
            requestReader.readFinished();
        }
        //we need to use the pooled result here because another write might be currently active
        final ProcessResponseResult result = resultPool.borrowObject();
        result.responseHolder.setService(serviceId);
        result.responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
        result.responseHolder.setSequence(sequence);
        result.responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ, message);
        writeQueue.add(result);
    }

    @Override
    public boolean isHeartbeatTimeout() {
        return endpointSession.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private boolean isRequestTimeout() {
        return endpointSession.getRequestTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    private void processResponse(final ProcessResponseResult result) {
        final int serviceId = result.requestHolder.getService();
        if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return;
        }
        final SynchronousEndpointService service = parent.getService(serviceId);
        if (service == null) {
            result.responseHolder.setService(serviceId);
            result.responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            result.responseHolder.setSequence(result.requestHolder.getSequence());
            result.responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "service not found: " + serviceId);
            writeQueue.add(result);
            return;
        }
        if (isRequestTimeout()) {
            result.responseHolder.setService(serviceId);
            result.responseHolder.setMethod(IServiceSynchronousCommand.RETRY_ERROR_METHOD_ID);
            result.responseHolder.setSequence(result.requestHolder.getSequence());
            result.responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "request timeout [" + endpointSession.getRequestTimeout() + "] exceeded, please try again later");
            writeQueue.add(result);
            return;
        }
        service.invoke(endpointSession.getSessionId(), result.requestHolder, result.responseHolder);
        writeQueue.add(result);
    }

    private class ProcessResponseResult implements INode<ProcessResponseResult> {
        private final CopyBufferServiceSynchronousCommand requestHolder = new CopyBufferServiceSynchronousCommand();
        private final ISerializingServiceSynchronousCommand<Object> responseHolder = newResponseHolder();
        @GuardedBy("only the IO thread has access to the future")
        private ListenableFuture<?> future;
        @GuardedBy("only the IO thread has access to this flag")
        private boolean writing;
        private ProcessResponseResult next;

        @Override
        public ProcessResponseResult getNext() {
            return next;
        }

        @Override
        public void setNext(final ProcessResponseResult next) {
            this.next = next;
        }

        public void close() {
            future = null;
            writing = false;
            requestHolder.close();
            responseHolder.close();
        }
    }

}
