package de.invesdwin.context.integration.channel.rpc.endpoint.session;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.service.command.ServiceCommandSynchronousReader;
import de.invesdwin.context.integration.channel.rpc.service.command.ServiceCommandSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
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
    public ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newRequestWriter() {
        final ISynchronousWriter<IByteBufferProvider> transport = endpoint.getWriter();
        return new ServiceCommandSynchronousWriter<IByteBufferProvider>(transport, ByteBufferProviderSerde.GET,
                ByteBuffers.EXPANDABLE_LENGTH);
    }

    @Override
    public ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newRequestReader() {
        final ISynchronousReader<IByteBufferProvider> transport = endpoint.getReader();
        return new ServiceCommandSynchronousReader<IByteBufferProvider>(transport, ByteBufferProviderSerde.GET);
    }

    @Override
    public ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newResponseWriter() {
        return newRequestWriter();
    }

    @Override
    public ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newResponseReader() {
        return newRequestReader();
    }

    @Override
    public void close() throws IOException {
        if (endpoint != null) {
            endpoint.close();
        }
    }

}
