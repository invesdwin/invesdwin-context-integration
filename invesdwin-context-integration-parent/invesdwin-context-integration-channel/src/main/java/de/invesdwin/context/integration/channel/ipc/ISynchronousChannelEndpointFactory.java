package de.invesdwin.context.integration.channel.ipc;

public interface ISynchronousChannelEndpointFactory<R, W> {

    ISynchronousChannelEndpoint<R, W> newEndpoint();

}
