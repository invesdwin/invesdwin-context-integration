package de.invesdwin.context.integration.grid.ignite3.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.grid.ignite3.server.Ignite3ServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@ThreadSafe
public class Ignite3ServerDiscoveryServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return Ignite3ServerProperties.getServerDiscoveryBindUri();
    }

}