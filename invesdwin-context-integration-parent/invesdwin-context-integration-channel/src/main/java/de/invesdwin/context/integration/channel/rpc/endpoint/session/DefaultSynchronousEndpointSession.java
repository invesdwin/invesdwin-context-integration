package de.invesdwin.context.integration.channel.rpc.endpoint.session;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceCommandSynchronousReader;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceCommandSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * The outer endpoint is used as the transport without any additional handshaking or resources associated with it.
 */
@NotThreadSafe
public class DefaultSynchronousEndpointSession implements ISynchronousEndpointSession {

    private final String sessionId;
    private final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint;

    public DefaultSynchronousEndpointSession(final String sessionId,
            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint) {
        this.sessionId = sessionId;
        this.endpoint = endpoint;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> getEndpoint() {
        return endpoint;
    }

    @Override
    public String toString() {
        return sessionId;
    }

    @Override
    public <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newRequestWriter(final ISerde<T> requestSerde) {
        final ISynchronousWriter<IByteBufferProvider> transport = endpoint.getWriter();
        return new ServiceCommandSynchronousWriter<T>(transport, requestSerde, ByteBuffers.EXPANDABLE_LENGTH);
    }

    @Override
    public <T> ISynchronousReader<IServiceSynchronousCommand<T>> newRequestReader(final ISerde<T> requestSerde) {
        final ISynchronousReader<IByteBufferProvider> transport = endpoint.getReader();
        return new ServiceCommandSynchronousReader<T>(transport, requestSerde);
    }

    @Override
    public <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newResponseWriter(final ISerde<T> responseSerde) {
        return newRequestWriter(responseSerde);
    }

    @Override
    public <T> ISynchronousReader<IServiceSynchronousCommand<T>> newResponseReader(final ISerde<T> responseSerde) {
        return newRequestReader(responseSerde);
    }

    @Override
    public void close() throws IOException {
        if (endpoint != null) {
            endpoint.close();
        }
    }

}
