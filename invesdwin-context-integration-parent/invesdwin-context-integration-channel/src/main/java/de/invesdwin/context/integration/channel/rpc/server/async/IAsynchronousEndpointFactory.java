package de.invesdwin.context.integration.channel.rpc.server.async;

public interface IAsynchronousEndpointFactory<R, W, O> {

    IAsynchronousEndpoint<R, W, O> newEndpoint();

}