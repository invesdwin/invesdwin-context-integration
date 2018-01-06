package de.invesdwin.context.integration.ftp;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class FtpProperties {

    public static final String SERVICE_NAME = "invesdwin-context-integration-ftp-server";
    public static final boolean INITIALIZED;

    static {
        final SystemProperties systemProperties = new SystemProperties();
        INITIALIZED = true;
    }

    private FtpProperties() {}

    private static void maybeValidatePort(final SystemProperties systemProperties, final String portKey) {
        if (systemProperties.containsKey(portKey)) {
            final Integer port = systemProperties.getInteger(portKey);
            if (port == 0) {
                //use random port
                systemProperties.getPort(portKey, true);
            }
        }
    }

}
