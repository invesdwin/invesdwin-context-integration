package de.invesdwin.integration.jppf;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.TypedProperties;
import org.jppf.utils.configuration.JPPFProperties;
import org.jppf.utils.configuration.JPPFProperty;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class JPPFClientProperties {

    public static final boolean INITIALIZED;

    static {
        //override values as defined in distribution/user properties
        final TypedProperties props = JPPFConfiguration.getProperties();
        final SystemProperties systemProperties = new SystemProperties();
        for (final JPPFProperty<?> property : JPPFProperties.allProperties()) {
            final String key = property.getName();
            if (systemProperties.containsKey(key)) {
                final String value = systemProperties.getString(key);
                props.setString(key, value);
            }
        }
        INITIALIZED = true;
    }

    private JPPFClientProperties() {}

}
