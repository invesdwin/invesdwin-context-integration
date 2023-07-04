package de.invesdwin.context.integration.channel.rpc.server.sessionless;

public interface ISessionlessSynchronousEndpointFactory<R, W, O> {

    ISessionlessSynchronousEndpoint<R, W, O> newEndpoint();

}