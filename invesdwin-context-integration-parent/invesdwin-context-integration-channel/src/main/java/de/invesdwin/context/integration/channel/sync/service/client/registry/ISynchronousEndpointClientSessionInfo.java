package de.invesdwin.context.integration.channel.sync.service.client.registry;

import java.io.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.service.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.sync.service.command.IServiceSynchronousCommand;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public interface ISynchronousEndpointClientSessionInfo extends Closeable {

    Duration DEFAULT_REQUEST_TIMEOUT = new Duration(3, FTimeUnit.MINUTES);
    Duration DEFAULT_REQUEST_WAIT_INTERVAL = new Duration(1, FTimeUnit.SECONDS);
    Duration DEFAULT_HEARTBEAT_INTERVAL = new Duration(30, FTimeUnit.SECONDS);
    Duration DEFAULT_HEARTBEAT_TIMEOUT = new Duration(5, FTimeUnit.MINUTES);

    String getSessionId();

    default Duration getRequestTimeout() {
        return DEFAULT_REQUEST_TIMEOUT;
    }

    default Duration getRequestWaitInterval() {
        return DEFAULT_REQUEST_WAIT_INTERVAL;
    }

    default Duration getHeartbeatInterval() {
        return DEFAULT_HEARTBEAT_INTERVAL;
    }

    default Duration getHeartbeatTimeout() {
        return DEFAULT_HEARTBEAT_TIMEOUT;
    }

    ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newRequestWriter(
            ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint);

    ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newResponseReader(
            ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint);

    ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newResponseWriter(
            ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint);

    ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newRequestReader(
            ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint);

    @Override
    void close();

}
