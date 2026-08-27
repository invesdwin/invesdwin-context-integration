package de.invesdwin.context.integration.grid.ignite2.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class Ignite2ServerProperties {
    public static final boolean STARTUP_ENABLED;
    public static final int SERVER_DISCOVERY_PORT;
    public static final int SERVER_COMMUNICATION_PORT;
    public static final int THIN_CLIENT_PORT;
    public static final int REST_PORT;
    public static final boolean INITIALIZED;

    static {
        final SystemProperties systemProperties = new SystemProperties(Ignite2ServerProperties.class);
        STARTUP_ENABLED = systemProperties.getBoolean("STARTUP_ENABLED");
        SERVER_DISCOVERY_PORT = systemProperties.getPort("SERVER_DISCOVERY_PORT", true);
        SERVER_COMMUNICATION_PORT = systemProperties.getPort("SERVER_COMMUNICATION_PORT", true);
        THIN_CLIENT_PORT = systemProperties.getPort("THIN_CLIENT_PORT", true);
        REST_PORT = systemProperties.getPort("REST_PORT", true);
        INITIALIZED = true;
    }

    private Ignite2ServerProperties() {}

    public static URI getServerDiscoveryBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + SERVER_DISCOVERY_PORT);
    }

    public static URI getThinClientBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + THIN_CLIENT_PORT);
    }

    public static URI getRestBindUri() {
        return URIs.asUri("http://" + IntegrationProperties.HOSTNAME + ":" + REST_PORT);
    }
}