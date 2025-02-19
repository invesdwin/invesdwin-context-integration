package de.invesdwin.context.integration.channel.rpc.base.client.session;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.AbortRequestException;
import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousEndpointClientSession extends Closeable {

    WrappedScheduledExecutorService HEARTBEAT_EXECUTOR = Executors
            .newScheduledThreadPool(ISynchronousEndpointClientSession.class.getSimpleName() + "_HEARTBEAT", 1);

    ISynchronousEndpointSession getEndpointSession();

    @Override
    void close();

    boolean isClosed();

    int nextRequestSequence();

    int getRequestSequence();

    void setRequestSequence(int sequence);

    int nextStreamSequence();

    int getStreamSequence();

    void setStreamSequence(int sequence);

    Duration getDefaultRequestTimeout();

    ICloseableByteBufferProvider request(int serviceId, int methodId, int requestSequence, IByteBufferProvider request,
            boolean closeRequest, Duration requestTimeout, boolean waitForResponse,
            IUnexpectedMessageListener unexpectedMessageListener) throws TimeoutException, AbortRequestException;

}
