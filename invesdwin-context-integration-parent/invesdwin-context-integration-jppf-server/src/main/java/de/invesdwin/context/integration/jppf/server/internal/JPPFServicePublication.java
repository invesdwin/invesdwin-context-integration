package de.invesdwin.context.integration.jppf.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.jppf.server.JPPFServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@ThreadSafe
public class JPPFServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return JPPFServerProperties.getServerBindUri();
    }

}