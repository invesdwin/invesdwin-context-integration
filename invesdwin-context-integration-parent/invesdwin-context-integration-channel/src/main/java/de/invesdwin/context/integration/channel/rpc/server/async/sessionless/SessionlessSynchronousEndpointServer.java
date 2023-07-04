package de.invesdwin.context.integration.channel.rpc.server.async.sessionless;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import com.google.common.util.concurrent.ListenableFuture;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.server.SynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.server.async.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.async.poll.SyncPollingQueueProvider;
import de.invesdwin.context.integration.channel.rpc.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.attributes.AttributesMap;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class SessionlessSynchronousEndpointServer implements ISynchronousChannel {

    private final AsynchronousEndpointServerHandlerFactory handlerFactory;
    private final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, Object> serverSessionFactory;
    private final WrappedExecutorService ioExecutor;
    private final SyncPollingQueueProvider pollingQueueProvider = new SyncPollingQueueProvider();
    private ISessionlessSynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, Object> serverEndpoint;
    private ISynchronousReader<IByteBufferProvider> requestReader;
    private ISynchronousWriter<IByteBufferProvider> responseWriter;
    private IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;
    private IoRunnable ioRunnable;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public SessionlessSynchronousEndpointServer(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverSessionFactory,
            final AsynchronousEndpointServerHandlerFactory handlerFactory) {
        this.serverSessionFactory = (ISessionlessSynchronousEndpointFactory) serverSessionFactory;
        this.handlerFactory = handlerFactory;
        this.ioExecutor = newIoExecutor();
    }

    /**
     * Should always be a CachedExecutorService (default)
     */
    protected WrappedExecutorService newIoExecutor() {
        return SynchronousEndpointServer.DEFAULT_IO_EXECUTOR;
    }

    public WrappedExecutorService getIoExecutor() {
        return ioExecutor;
    }

    @Override
    public void open() throws IOException {
        this.serverEndpoint = serverSessionFactory.newEndpoint();
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
        private final ManyToOneConcurrentLinkedQueue<Context> writeQueue = new ManyToOneConcurrentLinkedQueue<>();

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
            final Context writeTask = writeQueue.peek();
            if (writeTask != null) {
                /*
                 * even if we finish writing the current task and no other task follows, we still handled something and
                 * continue eagerly looking for more work (client might immediately give us a new task)
                 */
                writing = true;
                if (responseWriter.writeFlushed() && responseWriter.writeReady()) {
                    if (writeTask.result.isWriting()) {
                        final Context removedTask = writeQueue.remove();
                        final Context nextWriteTask = writeQueue.peek();
                        if (nextWriteTask != null) {
                            serverEndpoint.setOtherRemoteAddress(nextWriteTask.otherRemoteAddress);
                            responseWriter.write(nextWriteTask.response);
                            nextWriteTask.result.setWriting(true);
                        }
                        writeTask.close();
                        Assertions.checkSame(writeTask, removedTask);
                    } else {
                        serverEndpoint.setOtherRemoteAddress(writeTask.otherRemoteAddress);
                        responseWriter.write(writeTask.response);
                        writeTask.result.setWriting(true);
                    }
                }
            } else {
                //reading could still indicate that we are busy handling work
                writing = false;
            }
            if (requestReader.hasNext()) {
                final IByteBufferProvider request = requestReader.readMessage();
                try {
                    //TODO: make the context pooled and return the pooled instance after write is finished
                    final Context context = new Context();
                    context.otherRemoteAddress = serverEndpoint.getOtherRemoteAddress();
                    context.writeQueue = writeQueue;
                    final IByteBufferProvider response = handler.handle(context, request);
                    if (response != null) {
                        context.write(response);
                    }
                } catch (final EOFException e) {
                    close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    requestReader.readFinished();
                }
                return true;
            } else {
                return writing;
            }
        }

    }

    private static final class Context implements IAsynchronousHandlerContext<IByteBufferProvider> {
        private final ProcessResponseResult result = new ProcessResponseResult();
        private AttributesMap attributes;
        private Object otherRemoteAddress;
        private IByteBufferProvider response;
        private ManyToOneConcurrentLinkedQueue<Context> writeQueue;

        @Override
        public void write(final IByteBufferProvider output) {
            if (response != null) {
                throw new IllegalStateException("can only write a single response");
            }
            response = output;
            writeQueue.add(this);
        }

        @Override
        public void close() throws IOException {
            otherRemoteAddress = null;
            result.clean();
            attributes = null;
            response = null;
        }

        @Override
        public String getSessionId() {
            return Objects.toString(otherRemoteAddress);
        }

        @Override
        public AttributesMap getAttributes() {
            if (attributes == null) {
                synchronized (this) {
                    if (attributes == null) {
                        attributes = new AttributesMap();
                    }
                }
            }
            return attributes;
        }

        @Override
        public ProcessResponseResult borrowResult() {
            return result;
        }

        @Override
        public void returnResult(final ProcessResponseResult result) {
            result.clean();
        }
    }
}
