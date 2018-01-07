package de.invesdwin.context.integration.ftp;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class FtpClientProperties {

    public static final String SERVICE_NAME = "invesdwin-context-integration-ftp-server";
    public static final String USERNAME;
    public static final String PASSWORD;

    static {
        final SystemProperties systemProperties = new SystemProperties(FtpClientProperties.class);
        USERNAME = systemProperties.getString("USERNAME");
        PASSWORD = systemProperties.getStringWithSecurityWarning("PASSWORD", "invesdwin");
    }

    private FtpClientProperties() {}

}
