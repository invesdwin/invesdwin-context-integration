package de.invesdwin.context.integration.channel.rpc.endpoint.sessionless;

import de.invesdwin.context.integration.channel.rpc.endpoint.ISynchronousEndpointFactory;

public interface ISessionlessSynchronousEndpointFactory<R, W, O> extends ISynchronousEndpointFactory<R, W> {

    @Override
    ISessionlessSynchronousEndpoint<R, W, O> newEndpoint();

}