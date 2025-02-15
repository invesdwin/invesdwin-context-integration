package de.invesdwin.context.integration.channel.stream.server;

import java.io.IOException;

import de.invesdwin.context.integration.channel.rpc.base.server.ISynchronousEndpointServer;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSessionManager;
import de.invesdwin.context.integration.channel.stream.server.session.manager.IStreamSynchronousEndpointSession;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.util.time.duration.Duration;

public interface IStreamSynchronousEndpointServer extends ISynchronousEndpointServer {

    IStreamSynchronousEndpointService getService(int serviceId);

    IStreamSynchronousEndpointService getOrCreateService(int serviceId, String topic, IProperties parameters)
            throws IOException;

    int getMaxSuccessivePushCountPerSession();

    int getMaxSuccessivePushCountPerSubscription();

    IStreamSessionManager newManager(IStreamSynchronousEndpointSession session);

    Duration getHeartbeatTimeout();

    Duration getRequestTimeout();

}
