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
    public static final boolean LOCAL_EXECUTION_ENABLED;
    public static final int LOCAL_EXECUTION_THREADS;

    static {
        final SystemProperties systemProperties = new SystemProperties();
        maybeValidatePort(systemProperties, JPPFProperties.SERVER_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.SERVER_SSL_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.MANAGEMENT_PORT.getName());
        maybeValidatePort(systemProperties, JPPFProperties.MANAGEMENT_SSL_PORT.getName());
        USER_NAME = systemProperties.getStringWithSecurityWarning("jppf.user.name", "invesdwin");

        final TypedProperties props = JPPFConfiguration.getProperties();
        //        if (!systemProperties.containsValue(JPPFProperties.RESOURCE_CACHE_DIR.getName())) {
        //            props.set(JPPFProperties.RESOURCE_CACHE_DIR, ContextProperties.TEMP_DIRECTORY.getAbsolutePath());
        //        }
        //override values as defined in distribution/user properties
        for (final JPPFProperty<?> property : JPPFProperties.allProperties()) {
            final String key = property.getName();
            if (systemProperties.containsKey(key)) {
                final String value = systemProperties.getString(key);
                props.setString(key, value);
            }
        }
        CLIENT_SSL_ENABLED = props.get(JPPFProperties.SSL_ENABLED);
        LOCAL_EXECUTION_ENABLED = props.get(JPPFProperties.LOCAL_EXECUTION_ENABLED);
        LOCAL_EXECUTION_THREADS = props.get(JPPFProperties.LOCAL_EXECUTION_THREADS);
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
