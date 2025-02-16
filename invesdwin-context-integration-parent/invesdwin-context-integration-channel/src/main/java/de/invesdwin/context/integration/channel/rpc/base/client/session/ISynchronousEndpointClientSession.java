package de.invesdwin.context.integration.channel.rpc.base.client.session;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import de.invesdwin.context.integration.channel.rpc.base.client.session.unexpected.IUnexpectedMessageListener;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;
import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousEndpointClientSession extends Closeable {

    WrappedScheduledExecutorService HEARTBEAT_EXECUTOR = Executors
            .newScheduledThreadPool(ISynchronousEndpointClientSession.class.getSimpleName() + "_HEARTBEAT", 1);

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

    ICloseableByteBufferProvider request(int serviceId, int methodId, IByteBufferProvider request, int requestSequence,
            Duration requestTimeout, IUnexpectedMessageListener unexpectedMessageListener) throws TimeoutException;

}
