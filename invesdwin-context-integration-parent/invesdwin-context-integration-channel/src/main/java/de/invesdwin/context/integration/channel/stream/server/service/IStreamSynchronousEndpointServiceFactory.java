package de.invesdwin.context.integration.channel.stream.server.service;

import de.invesdwin.context.system.properties.IProperties;

public interface IStreamSynchronousEndpointServiceFactory {

    IStreamSynchronousEndpointService newService(int serviceId, String topic, IProperties properties);

}
