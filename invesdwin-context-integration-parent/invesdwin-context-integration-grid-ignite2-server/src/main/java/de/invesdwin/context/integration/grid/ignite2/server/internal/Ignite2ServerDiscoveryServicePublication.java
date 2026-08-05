package de.invesdwin.context.integration.grid.ignite2.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.grid.ignite2.server.Ignite2ServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@ThreadSafe
public class Ignite2ServerDiscoveryServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return Ignite2ServerProperties.getServerDiscoveryBindUri();
    }

}