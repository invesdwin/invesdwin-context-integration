package de.invesdwin.context.integration.channel.stream.server.async;

import java.io.Closeable;
import java.io.IOException;

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
import de.invesdwin.util.lang.BroadcastingCloseable;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public class StreamAsynchronousEndpointServerHandlerSession extends BroadcastingCloseable
        implements IStreamSynchronousEndpointSession, Closeable {

    public static final StreamAsynchronousEndpointServerHandlerSession[] EMPTY_ARRAY = new StreamAsynchronousEndpointServerHandlerSession[0];

    private final IStreamSynchronousEndpointServer server;
    private final IAsynchronousHandlerContext<IByteBufferProvider> context;
    private final Duration heartbeatTimeout;
    private final IStreamSessionManager manager;
    private long lastHeartbeatNanos = System.nanoTime();
    private int pushedMessages = 0;

    private volatile boolean closed;

    public StreamAsynchronousEndpointServerHandlerSession(final IStreamSynchronousEndpointServer server,
            final IAsynchronousHandlerContext<IByteBufferProvider> context, final Duration heartbeatTimeout) {
        this.server = server;
        this.context = context.asImmutable();
        this.heartbeatTimeout = heartbeatTimeout;
        this.manager = server.newManager(this);
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
        final ProcessResponseResult result = context.borrowResult();
        result.setContext(context);
        final EagerSerializingServiceSynchronousCommand<Object> response = result.getResponse();
        response.setService(service.getServiceId());
        response.setMethod(StreamServerMethodInfo.METHOD_ID_PUSH);
        /*
         * add a sequence to the pushed messages so that the client can validate if he missed some messages and
         * re-request them by resubscribing with his last known timestamp as a limiter in the subscription request or by
         * resetting the subscription entirely
         */
        response.setSequence(pushedMessages++);

        final IByteBufferProvider message = reader.readMessage();
        try {
            response.setMessageBuffer(message);
        } finally {
            reader.readFinished();
        }
        context.write(response.asBuffer());
        return true;
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

    public Duration getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setLastHeartbeatNanos(final long lastHeartbeatNanos) {
        this.lastHeartbeatNanos = lastHeartbeatNanos;
    }

}
