package de.invesdwin.context.integration.grid.ignite2.node;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class Ignite2NodeProperties {
    public static final boolean STARTUP_ENABLED;
    public static final int NODE_DISCOVERY_PORT;
    public static final int THIN_CLIENT_PORT;
    public static final boolean INITIALIZED;

    static {
        final SystemProperties systemProperties = new SystemProperties(Ignite2NodeProperties.class);
        STARTUP_ENABLED = systemProperties.getBoolean("STARTUP_ENABLED");
        NODE_DISCOVERY_PORT = systemProperties.getPort("NODE_DISCOVERY_PORT", true);
        THIN_CLIENT_PORT = systemProperties.getPort("THIN_CLIENT_PORT", true);
        INITIALIZED = true;
    }

    private Ignite2NodeProperties() {}

}