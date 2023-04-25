package de.invesdwin.context.integration.channel.sync.service.client.registry;

/**
 * Can be used to register/unregister additional resources (e.g. files that need to be deleted afterwards) for the given
 * client session. It can also be used to switch transports by using the outer endpoint for a handshake to determine
 * which other transport to use.
 */
public interface ISynchronousEndpointClientSessionRegistry {

    ISynchronousEndpointClientSessionInfo register(String sessionId);

    void unregister(String sessionId);

}
