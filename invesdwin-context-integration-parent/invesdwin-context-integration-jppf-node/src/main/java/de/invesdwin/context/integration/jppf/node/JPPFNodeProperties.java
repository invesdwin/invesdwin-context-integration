package de.invesdwin.context.integration.jppf.node;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;

@Immutable
public final class JPPFNodeProperties {

    public static final boolean STARTUP_ENABLED;
    public static final boolean PEER_SSL_ENABLED;
    public static final boolean INITIALIZED;
    public static final boolean RESET_TASK_CLASS_LOADER;

    static {
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        final SystemProperties systemProperties = new SystemProperties(JPPFNodeProperties.class);
        STARTUP_ENABLED = systemProperties.getBoolean("STARTUP_ENABLED");
        PEER_SSL_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.PEER_SSL_ENABLED);
        RESET_TASK_CLASS_LOADER = JPPFConfiguration.getProperties().get(JPPFProperties.CLASSLOADER_CACHE_SIZE) <= 1;
        INITIALIZED = true;
    }

    private JPPFNodeProperties() {}

}
