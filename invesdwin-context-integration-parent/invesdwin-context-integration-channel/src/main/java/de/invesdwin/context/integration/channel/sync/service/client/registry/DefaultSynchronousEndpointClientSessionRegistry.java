package de.invesdwin.context.integration.channel.sync.service.client.registry;

import javax.annotation.concurrent.Immutable;

/**
 * This is a pure client side session registry that just uses the outer transport for the actual communication.
 */
@Immutable
public class DefaultSynchronousEndpointClientSessionRegistry implements ISynchronousEndpointClientSessionRegistry {

    @Override
    public ISynchronousEndpointClientSessionInfo register(final String sessionId) {
        return new DefaultSynchronousEndpointClientSessionInfo(sessionId);
    }

    @Override
    public void unregister(final String sessionId) {
        //noop
    }

}
