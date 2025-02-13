package de.invesdwin.context.integration.channel.stream.server.service;

import java.util.Map;

public interface IStreamSynchronousEndpointServiceFactory {

    IStreamSynchronousEndpointService newService(int serviceId, String topic, Map<String, String> properties);

}
