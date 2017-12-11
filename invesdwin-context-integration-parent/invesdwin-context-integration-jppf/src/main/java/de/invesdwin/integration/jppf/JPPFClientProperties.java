package de.invesdwin.integration.jppf;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class JPPFClientProperties {

    public static final Boolean LOCAL_EXECUTION_ENABLED;

    static {
        final SystemProperties systemProperties = new SystemProperties(JPPFClientProperties.class);
        LOCAL_EXECUTION_ENABLED = systemProperties.getBoolean("LOCAL_EXECUTION_ENABLED");
    }

    private JPPFClientProperties() {}

}
