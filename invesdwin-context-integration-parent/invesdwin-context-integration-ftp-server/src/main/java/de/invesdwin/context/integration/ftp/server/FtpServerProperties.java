package de.invesdwin.context.integration.ftp.server;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class FtpServerProperties {

    public static final String SERVICE_NAME = "invesdwin-context-integration-ftp-server";
    public static final Integer PORT;

    static {
        final SystemProperties systemProperties = new SystemProperties();
        PORT = systemProperties.getPort("PORT", true);
    }

    private FtpServerProperties() {}

}
