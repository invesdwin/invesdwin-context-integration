package de.invesdwin.context.integration.grid.ignite2.node;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class Ignite2NodeProperties {
    public static final boolean STARTUP_ENABLED;
    public static final boolean CLIENT_MODE;
    public static final boolean FORCE_CLIENT_TO_SERVER_CONNECTIONS;
    public static final int NODE_DISCOVERY_PORT;
    public static final int THIN_CLIENT_PORT;
    public static final boolean INITIALIZED;

    static {
        final SystemProperties systemProperties = new SystemProperties(Ignite2NodeProperties.class);
        STARTUP_ENABLED = systemProperties.getBoolean("STARTUP_ENABLED");
        CLIENT_MODE = systemProperties.getBoolean("CLIENT_MODE");
        NODE_DISCOVERY_PORT = systemProperties.getPort("NODE_PORT", true);
        THIN_CLIENT_PORT = systemProperties.getPort("THIN_CLIENT_PORT", true);
        FORCE_CLIENT_TO_SERVER_CONNECTIONS = systemProperties.getBoolean("FORCE_CLIENT_TO_SERVER_CONNECTIONS");
        INITIALIZED = true;
    }

    private Ignite2NodeProperties() {}

    public static URI getNodeDiscoveryBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + NODE_DISCOVERY_PORT);
    }

    public static URI getThinClientBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + THIN_CLIENT_PORT);
    }
}