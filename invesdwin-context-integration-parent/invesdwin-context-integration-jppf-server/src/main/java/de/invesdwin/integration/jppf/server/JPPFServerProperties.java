package de.invesdwin.integration.jppf.server;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class JPPFServerProperties {

    public static final int SERVER_PORT;

    static {
        final SystemProperties systemProperties = new SystemProperties(JPPFServerProperties.class);
        SERVER_PORT = systemProperties.getPort("SERVER_PORT", true);
    }

    private JPPFServerProperties() {}

}
