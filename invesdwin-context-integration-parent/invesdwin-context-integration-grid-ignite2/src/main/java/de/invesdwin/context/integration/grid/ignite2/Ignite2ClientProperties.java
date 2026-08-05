package de.invesdwin.context.integration.grid.ignite2;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class Ignite2ClientProperties {
    public static final String THIN_CLIENT_SERVICE_NAME;
    public static final String SERVER_DISCOVERY_SERVICE_NAME;
    public static final boolean INITIALIZED;

    static {
        final SystemProperties systemProperties = new SystemProperties(Ignite2ClientProperties.class);
        THIN_CLIENT_SERVICE_NAME = systemProperties.getString("THIN_CLIENT_SERVICE_NAME");
        SERVER_DISCOVERY_SERVICE_NAME = systemProperties.getString("SERVER_DISCOVERY_SERVICE_NAME");
        INITIALIZED = true;
    }

    private Ignite2ClientProperties() {}
}