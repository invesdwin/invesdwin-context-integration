package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.context.integration.channel.rpc.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.util.time.duration.Duration;

public interface IAsynchronousHandlerFactory<I, O> extends Closeable {

    void open() throws IOException;

    IAsynchronousHandler<I, O> newHandler();

    default Duration getHeartbeatInterval() {
        return ISynchronousEndpointSession.DEFAULT_HEARTBEAT_INTERVAL;
    }

    default Duration getHeartbeatTimeout() {
        return ISynchronousEndpointSession.DEFAULT_HEARTBEAT_TIMEOUT;
    }

}
