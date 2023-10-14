package de.invesdwin.context.integration.channel.rpc.base.endpoint.session;

import java.io.Closeable;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
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

    ISynchronousWriter<IByteBufferProvider> newRequestWriter();

    default <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandRequestWriter(
            final ISerde<T> requestSerde) {
        return newCommandRequestWriter(newRequestWriter(), requestSerde);
    }

    <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandRequestWriter(
            ISynchronousWriter<IByteBufferProvider> requestWriter, ISerde<T> requestSerde);

    ISynchronousReader<IByteBufferProvider> newResponseReader();

    default <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandResponseReader(
            final ISerde<T> responseSerde) {
        return newCommandResponseReader(newResponseReader(), responseSerde);
    }

    <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandResponseReader(
            ISynchronousReader<IByteBufferProvider> responseReader, ISerde<T> responseSerde);

    ISynchronousWriter<IByteBufferProvider> newResponseWriter();

    default <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandResponseWriter(
            final ISerde<T> responseSerde) {
        return newCommandResponseWriter(newResponseWriter(), responseSerde);
    }

    <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandResponseWriter(
            ISynchronousWriter<IByteBufferProvider> responseWriter, ISerde<T> responseSerde);

    ISynchronousReader<IByteBufferProvider> newRequestReader();

    default <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandRequestReader(
            final ISerde<T> requestSerde) {
        return newCommandRequestReader(newRequestReader(), requestSerde);
    }

    <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandRequestReader(
            ISynchronousReader<IByteBufferProvider> requestReader, ISerde<T> requestSerde);
}
