package de.invesdwin.context.integration.channel.rpc.server.session;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.util.time.duration.Duration;

public interface ISynchronousEndpointServerSession extends Closeable {

    ISynchronousEndpointServerSession[] EMPTY_ARRAY = new ISynchronousEndpointServerSession[0];

    boolean isHeartbeatTimeout();

    Duration getHeartbeatTimeout();

    String getSessionId();

    boolean handle() throws IOException;

    boolean isClosed();

}
