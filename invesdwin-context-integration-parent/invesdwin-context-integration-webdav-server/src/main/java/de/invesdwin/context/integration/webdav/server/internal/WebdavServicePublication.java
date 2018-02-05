package de.invesdwin.context.integration.webdav.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.webdav.WebdavClientProperties;
import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@Named
@ThreadSafe
public class WebdavServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return WebdavServerProperties.getServerBindUri();
    }

    @Override
    public String getServiceName() {
        return WebdavClientProperties.SERVICE_NAME;
    }

}