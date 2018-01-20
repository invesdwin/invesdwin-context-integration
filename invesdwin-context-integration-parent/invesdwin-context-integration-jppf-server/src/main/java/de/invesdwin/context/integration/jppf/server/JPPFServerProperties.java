package de.invesdwin.context.integration.jppf.server;

import java.lang.reflect.Field;
import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.jppf.server.nio.classloader.ClassCache;
import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.jppf.JPPFClientProperties;
import de.invesdwin.context.integration.jppf.node.JPPFNodeProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class JPPFServerProperties {

    public static final boolean PEER_SSL_ENABLED;
    public static final boolean LOCAL_NODE_ENABLED;
    public static final boolean INITIALIZED;
    public static final boolean SERVER_CLASS_CACHE_ENABLED;

    static {
        Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
        PEER_SSL_ENABLED = JPPFNodeProperties.PEER_SSL_ENABLED;
        LOCAL_NODE_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.LOCAL_NODE_ENABLED);
        SERVER_CLASS_CACHE_ENABLED = new SystemProperties().getBoolean("jppf.server.class.cache.enabled");
        final Field enabledField = Reflections.findField(ClassCache.class, "enabled");
        Reflections.makeAccessible(enabledField);
        final boolean actualServerClassCacheEnabled = (boolean) Reflections.getField(enabledField, null);
        Assertions.assertThat(actualServerClassCacheEnabled).isEqualTo(SERVER_CLASS_CACHE_ENABLED);
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
