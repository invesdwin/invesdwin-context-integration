package de.invesdwin.context.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.node.JPPFNodeProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class JPPFServerProperties {

    public static final boolean STARTUP_ENABLED;
    public static final boolean PEER_SSL_ENABLED;
    public static final boolean LOCAL_NODE_ENABLED;
    public static final boolean INITIALIZED;
    public static final boolean SERVER_CLASS_CACHE_ENABLED;
    private static final String KEY_JPPF_SERVER_CLASS_CACHE_ENABLED = "jppf.server.class.cache.enabled";

    static {
        Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
        PEER_SSL_ENABLED = JPPFNodeProperties.PEER_SSL_ENABLED;
        LOCAL_NODE_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.LOCAL_NODE_ENABLED);
        final SystemProperties jppfSystemProperties = new SystemProperties();
        if (jppfSystemProperties.containsValue(KEY_JPPF_SERVER_CLASS_CACHE_ENABLED)) {
            SERVER_CLASS_CACHE_ENABLED = jppfSystemProperties.getBoolean(KEY_JPPF_SERVER_CLASS_CACHE_ENABLED);
        } else {
            //default in JPPF is true
            SERVER_CLASS_CACHE_ENABLED = true;
        }

        final SystemProperties systemProperties = new SystemProperties(JPPFServerProperties.class);
        STARTUP_ENABLED = systemProperties.getBoolean("STARTUP_ENABLED");
        INITIALIZED = true;
    }

    private JPPFServerProperties() {}

    public static URI getServerBindUri() {
        final int port;
        if (JPPFClientProperties.CLIENT_SSL_ENABLED) {
            port = JPPFConfiguration.get(JPPFProperties.SERVER_SSL_PORT);
        } else {
            port = JPPFConfiguration.get(JPPFProperties.SERVER_PORT);
        }
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + port);
    }

}
