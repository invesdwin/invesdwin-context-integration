package de.invesdwin.context.integration.channel.rpc.server.session;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;

public interface ISynchronousEndpointServerSession extends Closeable {

    ISynchronousEndpointServerSession[] EMPTY_ARRAY = new ISynchronousEndpointServerSession[0];

    boolean isHeartbeatTimeout();

    ISynchronousEndpointSession getEndpointSession();

    boolean handle() throws IOException;

}
