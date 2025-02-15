package de.invesdwin.context.integration.channel.sync.timeseriesdb.service;

import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointService;
import de.invesdwin.context.integration.channel.stream.server.service.IStreamSynchronousEndpointServiceFactory;
import de.invesdwin.context.system.properties.IProperties;

public class TimeSeriesDBStreamSynchronousEndpointServiceFactory implements IStreamSynchronousEndpointServiceFactory {

    @Override
    public IStreamSynchronousEndpointService newService(final int serviceId, final String topic,
            final IProperties properties) {
        return new TimeSeriesDBStreamSynchronousEndpointService(serviceId, topic, properties);
    }

}
