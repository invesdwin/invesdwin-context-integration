package de.invesdwin.context.integration.ftp.server.internal;

import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.integration.ftp.FtpClientProperties;
import de.invesdwin.context.integration.ftp.server.ConfiguredFtpServer;
import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.context.integration.ws.registry.publication.WebServicePublicationSupport;

@Named(FtpClientProperties.SERVICE_NAME)
@ThreadSafe
public class FtpServicePublication extends WebServicePublicationSupport {

    @Override
    public URI getUri() {
        ConfiguredFtpServer.getInstance();
        return FtpServerProperties.getServerBindUri();
    }

}