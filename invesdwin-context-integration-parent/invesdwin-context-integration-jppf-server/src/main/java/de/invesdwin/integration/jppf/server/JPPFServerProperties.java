package de.invesdwin.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.integration.jppf.node.JPPFNodeProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class JPPFServerProperties {

    public static final boolean PEER_SSL_ENABLED;
    public static final boolean LOCAL_NODE_ENABLED;
    public static final boolean INITIALIZED;

    static {
        Assertions.checkTrue(JPPFNodeProperties.INITIALIZED);
        PEER_SSL_ENABLED = JPPFNodeProperties.PEER_SSL_ENABLED;
        LOCAL_NODE_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.LOCAL_NODE_ENABLED);
        INITIALIZED = true;
    }

    private JPPFServerProperties() {}

    public static URI getServerBindUri() {
        return URIs.asUri(
                "p://" + IntegrationProperties.HOSTNAME + ":" + JPPFConfiguration.get(JPPFProperties.SERVER_SSL_PORT));
    }

}
