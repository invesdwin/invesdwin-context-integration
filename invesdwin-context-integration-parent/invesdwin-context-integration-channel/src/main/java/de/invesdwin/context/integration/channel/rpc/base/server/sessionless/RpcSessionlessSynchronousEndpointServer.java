package de.invesdwin.context.integration.channel.rpc.base.server.sessionless;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.sessionless.ISessionlessSynchronousEndpointFactory;
import de.invesdwin.context.integration.channel.rpc.base.server.async.RpcAsynchronousEndpointServerHandlerFactory;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * A sessionless server is used for datagram connections that do not track individual connections for each client.
 */
@ThreadSafe
public class RpcSessionlessSynchronousEndpointServer extends ASessionlessSynchronousEndpointServer {

    public RpcSessionlessSynchronousEndpointServer(
            final ISessionlessSynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverEndpointFactory,
            final RpcAsynchronousEndpointServerHandlerFactory handlerFactory) {
        super(serverEndpointFactory, handlerFactory);
    }

}
