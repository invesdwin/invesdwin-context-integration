package de.invesdwin.context.integration.channel.stream.server.sessionless;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.sessionless.ASessionlessSynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.async.StreamAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * A sessionless server is used for datagram connections that do not track individual connections for each client.
 */
@ThreadSafe
public class StreamSessionlessSynchronousEndpointServer extends ASessionlessSynchronousEndpointServer {

    public StreamSessionlessSynchronousEndpointServer(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final StreamAsynchronousEndpointServerHandlerFactory handlerFactory) {
        super(serverEndpointFactory, handlerFactory);
    }

}
