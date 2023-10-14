package de.invesdwin.context.integration.channel.rpc.base.endpoint.session;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.ISynchronousEndpoint;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceCommandSynchronousReader;
import de.invesdwin.context.integration.channel.rpc.base.server.service.command.ServiceCommandSynchronousWriter;
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
    private ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint;

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
    public String toString() {
        return sessionId;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newRequestWriter() {
        return endpoint.getWriter();
    }

    @Override
    public <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandRequestWriter(
            final ISynchronousWriter<IByteBufferProvider> requestWriter, final ISerde<T> requestSerde) {
        return new ServiceCommandSynchronousWriter<T>(requestWriter, requestSerde, ByteBuffers.EXPANDABLE_LENGTH);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRequestReader() {
        return endpoint.getReader();
    }

    @Override
    public <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandRequestReader(
            final ISynchronousReader<IByteBufferProvider> requestReader, final ISerde<T> requestSerde) {
        return new ServiceCommandSynchronousReader<T>(requestReader, requestSerde);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newResponseWriter() {
        return newRequestWriter();
    }

    @Override
    public <T> ISynchronousWriter<IServiceSynchronousCommand<T>> newCommandResponseWriter(
            final ISynchronousWriter<IByteBufferProvider> responseWriter, final ISerde<T> responseSerde) {
        return newCommandRequestWriter(responseSerde);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newResponseReader() {
        return newRequestReader();
    }

    @Override
    public <T> ISynchronousReader<IServiceSynchronousCommand<T>> newCommandResponseReader(
            final ISynchronousReader<IByteBufferProvider> responseReader, final ISerde<T> responseSerde) {
        return newCommandRequestReader(responseSerde);
    }

    @Override
    public void close() throws IOException {
        if (endpoint != null) {
            endpoint.close();
            endpoint = null;
        }
    }

}
