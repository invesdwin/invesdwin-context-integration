package de.invesdwin.context.integration.channel.rpc.endpoint;

public interface ISynchronousEndpointFactory<R, W> {

    ISynchronousEndpoint<R, W> newEndpoint();

}
