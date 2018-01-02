package de.invesdwin.context.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@Named(JPPFClientProperties.SERVICE_NAME)
@ThreadSafe
public class JPPFServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        ConfiguredJPPFDriver.getInstance();
        return JPPFServerProperties.getServerBindUri();
    }

}