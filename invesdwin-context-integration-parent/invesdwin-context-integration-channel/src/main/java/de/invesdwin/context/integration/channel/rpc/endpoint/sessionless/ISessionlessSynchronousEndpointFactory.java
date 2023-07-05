package de.invesdwin.context.integration.channel.rpc.endpoint.sessionless;

public interface ISessionlessSynchronousEndpointFactory<R, W, O> {

    ISessionlessSynchronousEndpoint<R, W, O> newEndpoint();

}