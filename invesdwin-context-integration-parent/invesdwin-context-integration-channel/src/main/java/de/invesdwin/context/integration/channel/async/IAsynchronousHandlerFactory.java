package de.invesdwin.context.integration.channel.async;

import java.io.Closeable;
import java.io.IOException;

import de.invesdwin.context.integration.channel.rpc.base.endpoint.session.ISynchronousEndpointSession;
import de.invesdwin.util.time.duration.Duration;

public interface IAsynchronousHandlerFactory<I, O> extends Closeable {

    default Duration getHeartbeatInterval() {
        return ISynchronousEndpointSession.DEFAULT_HEARTBEAT_INTERVAL;
    }

    default Duration getHeartbeatTimeout() {
        return ISynchronousEndpointSession.DEFAULT_HEARTBEAT_TIMEOUT;
    }

    default Duration getRequestWaitInterval() {
        return ISynchronousEndpointSession.DEFAULT_REQUEST_WAIT_INTERVAL;
    }

    default Duration getRequestTimeout() {
        return ISynchronousEndpointSession.DEFAULT_REQUEST_TIMEOUT;
    }

    void open() throws IOException;

    IAsynchronousHandler<I, O> newHandler();

}
