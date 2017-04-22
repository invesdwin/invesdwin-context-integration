package de.invesdwin.common.integration.ws;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.fdate.FTimeUnit;

@Immutable
public final class IntegrationWsProperties {

    public static final long SERVICE_BINDING_HEARTBEAT_REFRESH_INVERVAL_MILLIS = 1 * FTimeUnit.MINUTES_IN_HOUR
            * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND; //4 Hours
    public static final long SERVICE_BINDING_HEARTBEAT_PURGE_INTERVAL_MILLIS = 6 * FTimeUnit.MINUTES_IN_HOUR
            * FTimeUnit.SECONDS_IN_MINUTE * FTimeUnit.MILLISECONDS_IN_SECOND; //6 Hours
    public static final String REGISTRY_SERVER_USER;
    public static final String REGISTRY_SERVER_PASSWORD;
    public static final String WSS_USERNAMETOKEN_USER;
    public static final String WSS_USERNAMETOKEN_PASSWORD;
    public static final String SPRING_WEB_USER;
    public static final String SPRING_WEB_PASSWORD;
    private static final SystemProperties SYSTEM_PROPERTIES = new SystemProperties(IntegrationWsProperties.class);

    static {
        REGISTRY_SERVER_USER = SYSTEM_PROPERTIES.getString("REGISTRY_SERVER_USER");
        REGISTRY_SERVER_PASSWORD = SYSTEM_PROPERTIES.getStringWithSecurityWarning("REGISTRY_SERVER_PASSWORD",
                IProperties.INVESDWIN_DEFAULT_PASSWORD);
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
        return URIs.asUri(SYSTEM_PROPERTIES.getString("REGISTRY_SERVER_URI"));
    }

    public static void setRegistryServerUri(final URI uri) {
        SYSTEM_PROPERTIES.setString("REGISTRY_SERVER_URI", uri.toString());
    }

}
