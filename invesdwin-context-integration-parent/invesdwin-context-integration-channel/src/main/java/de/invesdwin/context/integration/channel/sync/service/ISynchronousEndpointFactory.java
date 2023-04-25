package de.invesdwin.context.integration.channel.sync.service;

public interface ISynchronousEndpointFactory<R, W> {

    ISynchronousEndpoint<R, W> newEndpoint();

}
