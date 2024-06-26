package de.invesdwin.context.integration.channel.rpc.base.client.session;

import java.io.Closeable;

import de.invesdwin.context.integration.channel.rpc.base.client.handler.ClientMethodInfo;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedScheduledExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBufferProvider;

public interface ISynchronousEndpointClientSession extends Closeable {

    WrappedScheduledExecutorService HEARTBEAT_EXECUTOR = Executors
            .newScheduledThreadPool(ISynchronousEndpointClientSession.class.getSimpleName() + "_HEARTBEAT", 1);

    @Override
    void close();

    boolean isClosed();

    ICloseableByteBufferProvider request(ClientMethodInfo methodInfo, IByteBufferProvider request);

}
