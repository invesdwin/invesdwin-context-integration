package de.invesdwin.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@Named
@ThreadSafe
public class JPPFServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return JPPFServerProperties.getServerBindUri();
    }

}