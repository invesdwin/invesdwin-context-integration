package de.invesdwin.integration.jppf.node;

import javax.annotation.concurrent.Immutable;

import org.jppf.utils.JPPFConfiguration;
import org.jppf.utils.configuration.JPPFProperties;

import de.invesdwin.integration.jppf.JPPFClientProperties;
import de.invesdwin.util.assertions.Assertions;

@Immutable
public final class JPPFNodeProperties {

    public static final boolean PEER_SSL_ENABLED;
    public static final boolean INITIALIZED;

    static {
        Assertions.checkTrue(JPPFClientProperties.INITIALIZED);
        PEER_SSL_ENABLED = JPPFConfiguration.getProperties().get(JPPFProperties.PEER_SSL_ENABLED);
        INITIALIZED = true;
    }

    private JPPFNodeProperties() {}

}
