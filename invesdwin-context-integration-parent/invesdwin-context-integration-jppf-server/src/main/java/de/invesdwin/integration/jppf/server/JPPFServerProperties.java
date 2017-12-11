package de.invesdwin.integration.jppf.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class JPPFServerProperties {

    public static final int SERVER_PORT;

    static {
        Assertions.checkNotNull(JPPFClientProperties.class);
        final SystemProperties systemProperties = new SystemProperties(JPPFServerProperties.class);
        SERVER_PORT = systemProperties.getPort("SERVER_PORT", true);
    }

    private JPPFServerProperties() {}

    public URI getServerBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + SERVER_PORT);
    }

}
