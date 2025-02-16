package de.invesdwin.context.integration.channel.stream.server.async;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandlerContext;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.context.integration.channel.stream.server.IStreamSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.StreamServerMethodInfo;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.circular.CircularGenericArrayQueue;
import de.invesdwin.util.lang.BroadcastingCloseable;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

/**
 * For sessionless servers (e.g. datagram) that do not track sessions themselves, we have to track our own sessions so
 * we can handle subscriptions properly.
 */
@Immutable
public class StreamAsynchronousEndpointServerHandlerSession extends BroadcastingCloseable
        implements IStreamSynchronousEndpointSession, Closeable {

    public static final StreamAsynchronousEndpointServerHandlerSession[] EMPTY_ARRAY = new StreamAsynchronousEndpointServerHandlerSession[0];

    private final IStreamSynchronousEndpointServer server;
    private final IAsynchronousHandlerContext<IByteBufferProvider> context;
    private final Duration heartbeatTimeout;
    private final Duration requestTimeout;
    private final IStreamSessionManager manager;
    private long lastHeartbeatNanos = System.nanoTime();
    @GuardedBy("self")
    private final CircularGenericArrayQueue<Future<?>> pendingWrites;
    @GuardedBy("pendingWrites")
    private int streamSequenceCounter = 0;

    private volatile boolean closed;

    public StreamAsynchronousEndpointServerHandlerSession(final IStreamSynchronousEndpointServer server,
            final IAsynchronousHandlerContext<IByteBufferProvider> context) {
        this.server = server;
        this.context = context.asImmutable();
        this.heartbeatTimeout = server.getHeartbeatTimeout();
        this.requestTimeout = server.getRequestTimeout();
        this.manager = server.newManager(this);
        this.pendingWrites = new CircularGenericArrayQueue<Future<?>>(server.getMaxSuccessivePushCountPerSession());
    }

    @Override
    public IStreamSynchronousEndpointServer getServer() {
        return server;
    }

    public IAsynchronousHandlerContext<IByteBufferProvider> getContext() {
        return context;
    }

    public IStreamSessionManager getManager() {
        return manager;
    }

    @Override
    public boolean pushSubscriptionMessage(final IStreamSynchronousEndpointService service,
            final ISynchronousReader<IByteBufferProvider> reader) throws IOException {
        synchronized (pendingWrites) {
            if (!pendingWrites.isEmpty()) {
                do {
                    final Future<?> pendingWrite = pendingWrites.peek();
                    if (pendingWrite.isDone()) {
                        final Future<?> removed = pendingWrites.poll();
                        Assertions.checkSame(pendingWrite, removed);
                    } else {
                        //first-in-first-out
                        break;
                    }
                } while ((!pendingWrites.isEmpty()));
                if (pendingWrites.size() >= pendingWrites.capacity()) {
                    //session is too busy right now
                    return false;
                }
            }

            final ProcessResponseResult result = context.borrowResult();
            result.setContext(context);
            final EagerSerializingServiceSynchronousCommand<Object> response = result.getResponse();
            response.setService(service.getServiceId());
            response.setMethod(StreamServerMethodInfo.METHOD_ID_PUSH);
            /*
             * add a sequence to the pushed messages so that the client can validate if he missed some messages and
             * re-request them by resubscribing with his last known timestamp as a limiter in the subscription request
             * or by resetting the subscription entirely
             */
            response.setSequence(nextStreamSequence());

            final IByteBufferProvider message = reader.readMessage();
            try {
                response.setMessageBuffer(message);
            } finally {
                reader.readFinished();
            }
            final Future<?> pendingWrite = context.write(response.asBuffer());
            if (!pendingWrite.isDone()) {
                pendingWrites.add(pendingWrite);
            }
            return true;
        }
    }

    private int nextStreamSequence() {
        final int sequence = --streamSequenceCounter;
        if (sequence > 0) {
            //handle rollover
            streamSequenceCounter = -1;
            return -1;
        } else {
            return sequence;
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            super.close();
            try {
                manager.close();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isHeartbeatTimeout() {
        return getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    public boolean isRequestTimeout() {
        if (isClosed()) {
            return true;
        }
        return getRequestTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setLastHeartbeatNanos(final long lastHeartbeatNanos) {
        this.lastHeartbeatNanos = lastHeartbeatNanos;
    }

}
