package de.invesdwin.context.integration.ws;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.ws.registry.internal.RemoteRegistryService;
import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;
import de.invesdwin.util.time.date.FTimeUnit;

@Immutable
public final class IntegrationWsProperties {

    public static final long SERVICE_BINDING_HEARTBEAT_REFRESH_INVERVAL_MILLIS = 1 * FTimeUnit.SECONDS_IN_MINUTE
            * FTimeUnit.MILLISECONDS_IN_SECOND; //1 minute
    public static final long SERVICE_BINDING_HEARTBEAT_PURGE_INTERVAL_MILLIS = 5 * FTimeUnit.SECONDS_IN_MINUTE
            * FTimeUnit.MILLISECONDS_IN_SECOND; //5 minutes
    public static final String WSS_USERNAMETOKEN_USER;
    public static final String WSS_USERNAMETOKEN_PASSWORD;
    public static final String SPRING_WEB_USER;
    public static final String SPRING_WEB_PASSWORD;
    private static final SystemProperties SYSTEM_PROPERTIES = new SystemProperties(IntegrationWsProperties.class);

    private static final String KEY_REGISTRY_SERVER_URI = "REGISTRY_SERVER_URI";
    private static final String KEY_USE_REGISTRY_GATEWAY = "USE_REGISTRY_GATEWAY";

    static {
        WSS_USERNAMETOKEN_USER = SYSTEM_PROPERTIES.getString("WSS_USERNAMETOKEN_USER");
        WSS_USERNAMETOKEN_PASSWORD = SYSTEM_PROPERTIES.getStringWithSecurityWarning("WSS_USERNAMETOKEN_PASSWORD",
                IProperties.INVESDWIN_DEFAULT_PASSWORD);
        SPRING_WEB_USER = SYSTEM_PROPERTIES.getString("SPRING_WEB_USER");
        SPRING_WEB_PASSWORD = SYSTEM_PROPERTIES.getStringWithSecurityWarning("SPRING_WEB_PASSWORD",
                IProperties.INVESDWIN_DEFAULT_PASSWORD);
        //just validate that the property exists
        Assertions.assertThat(getRegistryServerUri()).isNotNull();
    }

    private IntegrationWsProperties() {}

    public static URI getRegistryServerUri() {
        return URIs.asUri(SYSTEM_PROPERTIES.getString(KEY_REGISTRY_SERVER_URI));
    }

    public static void setRegistryServerUri(final URI uri) {
        SYSTEM_PROPERTIES.setString(KEY_REGISTRY_SERVER_URI, uri.toString());
    }

    public static boolean isUseRegistryGateway() {
        if (SYSTEM_PROPERTIES.containsValue(KEY_USE_REGISTRY_GATEWAY)) {
            return SYSTEM_PROPERTIES.getBoolean(KEY_USE_REGISTRY_GATEWAY);
        } else {
            return false;
        }
    }

    public static void setUseRegistryGateway(final boolean useRegistryGateway) {
        SYSTEM_PROPERTIES.setBoolean(KEY_USE_REGISTRY_GATEWAY, useRegistryGateway);
    }

    public static IURIsConnect gateway(final IURIsConnect connect) {
        return RemoteRegistryService.gateway(connect);
    }

}
