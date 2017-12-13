package de.invesdwin.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class JPPFServerProperties {

    public static final boolean PEER_SSL_ENABLED;

    static {
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        PEER_SSL_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.PEER_SSL_ENABLED);
    }

    private JPPFServerProperties() {}

    public static URI getServerBindUri() {
        return URIs.asUri(
                "p://" + IntegrationProperties.HOSTNAME + ":" + JPPFConfiguration.get(JPPFProperties.SERVER_SSL_PORT));
    }

}
