package de.invesdwin.context.integration.ftp.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@ThreadSafe
public class FtpServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        return FtpServerProperties.getServerBindUri();
    }

}