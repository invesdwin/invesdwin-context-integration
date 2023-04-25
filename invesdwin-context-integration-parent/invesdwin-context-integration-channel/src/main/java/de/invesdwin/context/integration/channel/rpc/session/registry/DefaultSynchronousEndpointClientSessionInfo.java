package de.invesdwin.context.integration.channel.rpc.session.registry;

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
public class DefaultSynchronousEndpointClientSessionInfo implements ISynchronousEndpointClientSessionInfo {

    private final String sessionId;

    public DefaultSynchronousEndpointClientSessionInfo(final String sessionId) {
        this.sessionId = sessionId;
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
    public ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newRequestWriter(
            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint) {
        final ISynchronousWriter<IByteBufferProvider> transport = endpoint.getWriter();
        return new ServiceCommandSynchronousWriter<IByteBufferProvider>(transport, ByteBufferProviderSerde.GET,
                ByteBuffers.EXPANDABLE_LENGTH);
    }

    @Override
    public ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newRequestReader(
            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint) {
        final ISynchronousReader<IByteBufferProvider> transport = endpoint.getReader();
        return new ServiceCommandSynchronousReader<IByteBufferProvider>(transport, ByteBufferProviderSerde.GET);
    }

    @Override
    public ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> newResponseWriter(
            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint) {
        return newRequestWriter(endpoint);
    }

    @Override
    public ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> newResponseReader(
            final ISynchronousEndpoint<IByteBufferProvider, IByteBufferProvider> endpoint) {
        return newRequestReader(endpoint);
    }

    @Override
    public void close() {
        //nothing to clean up here
    }

}
