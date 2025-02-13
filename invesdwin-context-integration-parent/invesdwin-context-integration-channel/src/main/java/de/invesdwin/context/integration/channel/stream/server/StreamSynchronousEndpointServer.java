package de.invesdwin.context.integration.channel.stream.server;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.context.integration.channel.rpc.base.server.ASynchronousEndpointServer;
import de.invesdwin.context.integration.channel.rpc.base.server.session.ISynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.session.MultiplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.stream.server.session.SingleplexingStreamSynchronousEndpointServerSession;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;

/**
 * TODO: switch to command messages so that different topics can be defined as "service"??
 * 
 * TODO: add topics that clients can subscribe to or write messages to
 * 
 * TODO: add an actual heartbeat message to the transport layer
 */
@ThreadSafe
public class StreamSynchronousEndpointServer extends ASynchronousEndpointServer {

    public StreamSynchronousEndpointServer(final ISynchronousReader<ISynchronousEndpointSession> serverAcceptor) {
        super(serverAcceptor);
    }

    @Override
    protected ISynchronousEndpointServerSession newServerSession(final ISynchronousEndpointSession endpointSession) {
        if (getWorkExecutor() == null) {
            /*
             * Singlexplexing can not handle more than 1 request at a time, so this is the most efficient. Though could
             * also be used with workExecutor to limit concurrent requests different to IO threads. But IO threads are
             * normally good enough when requests are not expensive. Though if there is a mix between expensive and fast
             * requests, then a work executor with Singleplexing might be preferable. In all other cases I guess
             * multiplexing should be favored.
             */
            return new SingleplexingStreamSynchronousEndpointServerSession(this, endpointSession);
        } else {
            //we want to be able to handle multiple
            return new MultiplexingStreamSynchronousEndpointServerSession(this, endpointSession);
        }
    }

}
