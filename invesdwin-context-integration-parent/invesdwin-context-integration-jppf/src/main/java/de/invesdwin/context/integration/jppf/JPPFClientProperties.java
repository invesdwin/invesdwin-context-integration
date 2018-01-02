package de.invesdwin.context.integration.jppf;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.jppf.utils.configuration.JPPFProperty;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class JPPFClientProperties {

    public static final String SERVICE_NAME = "invesdwin-context-integration-jppf-server";
    public static final boolean INITIALIZED;
    public static final boolean CLIENT_SSL_ENABLED;
    public static final String USER_NAME;

    static {
        final SystemProperties systemProperties = new SystemProperties();
        maybeValidatePort(systemProperties, JPPFProperties.SERVER_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.SERVER_SSL_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.MANAGEMENT_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.MANAGEMENT_SSL_PORT.getName());
        USER_NAME = systemProperties.getStringWithSecurityWarning("jppf.user.name", "invesdwin");

        //override values as defined in distribution/user properties
        final TypedProperties props = JPPFConfiguration.getProperties();
        for (final JPPFProperty<?> property : JPPFProperties.allProperties()) {
            final String key = property.getName();
            if (systemProperties.containsKey(key)) {
                final String value = systemProperties.getString(key);
                props.setString(key, value);
            }
        }
        CLIENT_SSL_ENABLED = props.get(JPPFProperties.SSL_ENABLED);
        INITIALIZED = true;
    }

    private JPPFClientProperties() {}

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
