package de.invesdwin.context.integration.channel.rpc.endpoint.session;

import java.io.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public interface ISynchronousEndpointSession extends Closeable {

    Duration DEFAULT_REQUEST_TIMEOUT = new Duration(3, FTimeUnit.MINUTES);
    Duration DEFAULT_REQUEST_WAIT_INTERVAL = new Duration(1, FTimeUnit.SECONDS);
    Duration DEFAULT_HEARTBEAT_INTERVAL = new Duration(30, FTimeUnit.SECONDS);
    Duration DEFAULT_HEARTBEAT_TIMEOUT = new Duration(5, FTimeUnit.MINUTES);

    String getSessionId();

    ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> getEndpoint();

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

    <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newRequestWriter(ISerde<T> requestSerde);

    <T> ISynchronousReader<IServiceSynchronousCommand<T>> newResponseReader(ISerde<T> responseSerde);

    <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newResponseWriter(ISerde<T> requestSerde);

    <T> ISynchronousReader<IServiceSynchronousCommand<T>> newRequestReader(ISerde<T> responseSerde);

}
