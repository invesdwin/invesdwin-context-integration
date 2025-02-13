package de.invesdwin.context.integration.channel.rpc.base.server.sessionless;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.RpcSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.async.IAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.poll.SyncPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context.SessionlessHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.sessionless.context.SessionlessHandlerContextPool;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * A sessionless server is used for datagram connections that do not track individual connections for each client.
 */
@ThreadSafe
public abstract class ASessionlessSynchronousEndpointServer implements ISynchronousChannel {

    private final IAsynchronousEndpointServerHandlerFactory handlerFactory;
    private final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, Object> serverEndpointFactory;
    private final WrappedExecutorService ioExecutor;
    private final SyncPollingQueueProvider pollingQueueProvider = new SyncPollingQueueProvider();
    private ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, Object> serverEndpoint;
    private ISynchronousReader<IByteBufferProvider> requestReader;
    private ISynchronousWriter<IByteBufferProvider> responseWriter;
    private IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
    private IoRunnable ioRunnable;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ASessionlessSynchronousEndpointServer(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final IAsynchronousEndpointServerHandlerFactory handlerFactory) {
        this.serverEndpointFactory = (ISessionlessSynchronousEndpointFactory) serverEndpointFactory;
        this.handlerFactory = handlerFactory;
        this.ioExecutor = newIoExecutor();
    }

    /**
     * Should always be a CachedExecutorService (default)
     */
    protected WrappedExecutorService newIoExecutor() {
        return RpcSynchronousEndpointServer.DEFAULT_IO_EXECUTOR;
    }

    public WrappedExecutorService getIoExecutor() {
        return ioExecutor;
    }

    @Override
    public void open() throws IOException {
        this.serverEndpoint = serverEndpointFactory.newEndpoint();
        this.requestReader = serverEndpoint.getReader();
        this.responseWriter = serverEndpoint.getWriter();
        this.requestReader.open();
        this.responseWriter.open();

        handlerFactory.setPollingQueueProvider(pollingQueueProvider);
        this.handler = handlerFactory.newHandler();

        ioRunnable = new IoRunnable();
        final ListenableFuture<?> future = getIoExecutor().submit(ioRunnable);
        ioRunnable.setFuture(future);
    }

    @Override
    public void close() throws IOException {
        if (ioRunnable != null) {
            ioRunnable.close();
            ioRunnable = null;
        }
    }

    private final class IoRunnable implements Runnable, Closeable {
        private final ASpinWait throttle = new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                //throttle while nothing to do, spin quickly while work is available
                boolean handledOverall = false;
                boolean handledNow;
                do {
                    handledNow = handle();
                    handledOverall |= handledNow;
                } while (handledNow);
                return handledOverall;
            }
        };
        /*
         * We don't use a bounded sized queue here because one slow client might otherwise block worker threads that
         * could still work on other tasks for other clients. PendingCount check will just
         */
        private final ManyToOneConcurrentLinkedQueue<SessionlessHandlerContext> writeQueue = new ManyToOneConcurrentLinkedQueue<>();

        private volatile Future<?> future;

        public void setFuture(final Future<?> future) {
            this.future = future;
        }

        @Override
        public synchronized void close() {
            if (future != null) {
                future.cancel(true);
                future = null;
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    throttle.awaitFulfill(System.nanoTime(), handlerFactory.getRequestWaitInterval());
                }
            } catch (final Throwable t) {
                if (Throwables.isCausedByInterrupt(t)) {
                    //end
                    return;
                } else {
                    Err.process(new RuntimeException("ignoring", t));
                }
            } finally {
                future = null;
                if (handler != null) {
                    Closeables.closeQuietly(handler);
                    handler = null;
                }
                if (requestReader != null) {
                    Closeables.closeQuietly(requestReader);
                    requestReader = null;
                }
                if (responseWriter != null) {
                    Closeables.closeQuietly(responseWriter);
                    responseWriter = null;
                }
                if (serverEndpoint != null) {
                    Closeables.closeQuietly(serverEndpoint);
                    serverEndpoint = null;
                }
            }
        }

        private boolean handle() throws IOException {
            boolean writing = pollingQueueProvider.maybePollResults();
            final SessionlessHandlerContext writeTask = writeQueue.peek();
            if (writeTask != null) {
                /*
                 * even if we finish writing the current task and no other task follows, we still handled something and
                 * continue eagerly looking for more work (client might immediately give us a new task)
                 */
                writing = true;
                if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                    if (writeTask.getResult().isWriting()) {
                        final SessionlessHandlerContext removedTask = writeQueue.remove();
                        final SessionlessHandlerContext nextWriteTask = writeQueue.peek();
                        if (nextWriteTask != null) {
                            serverEndpoint.setOtherSocketAddress(nextWriteTask.getOtherSocketAddress());
                            responseWriter.write(nextWriteTask.getResponse());
                            nextWriteTask.getResult().setWriting(true);
                        }
                        writeTask.close();
                        Assertions.checkSame(writeTask, removedTask);
                    } else {
                        serverEndpoint.setOtherSocketAddress(writeTask.getOtherSocketAddress());
                        responseWriter.write(writeTask.getResponse());
                        writeTask.getResult().setWriting(true);
                    }
                }
            } else {
                //reading could still indicate that we are busy handling work
                writing = false;
            }
            try {
                if (requestReader.hasNext()) {
                    final IByteBufferProvider request = requestReader.readMessage();
                    final SessionlessHandlerContext context = SessionlessHandlerContextPool.INSTANCE.borrowObject();
                    try {
                        context.init(serverEndpoint.getOtherSocketAddress(), writeQueue);
                        final IByteBufferProvider response = handler.handle(context, request);
                        if (response != null) {
                            try {
                                context.write(response);
                            } finally {
                                /*
                                 * WARNING: this might cause problems if the handler reuses output buffers, since we
                                 * don't make a safe copy here for the write queue and further requests could come in.
                                 * This needs to be considered when modifying/wrapping the handler. To fix the issue,
                                 * ProcessResponseResult (via context.borrowResult() and result.close()) should be used
                                 * by the handler.
                                 */
                                handler.outputFinished(context);
                            }
                        }
                    } catch (final Throwable t) {
                        context.close();
                        throw Throwables.propagate(t);
                    } finally {
                        requestReader.readFinished();
                    }
                    return true;
                } else {
                    return writing;
                }
            } catch (final EOFException e) {
                close();
                return false;
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

}
