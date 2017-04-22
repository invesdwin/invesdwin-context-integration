package de.invesdwin.common.integration.amqp;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.system.properties.IProperties;
import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class AmqpClientProperties {

    public static final String HOST;
    public static final int PORT;
    public static final String USER;
    public static final String PASSWORD;

    static {
        final SystemProperties systemProperties = new SystemProperties(AmqpClientProperties.class);
        HOST = systemProperties.getString("HOST");
        PORT = systemProperties.getInteger("PORT");
        USER = systemProperties.getString("USER");
        PASSWORD = systemProperties.getStringWithSecurityWarning("PASSWORD", IProperties.INVESDWIN_DEFAULT_PASSWORD);
    }

    private AmqpClientProperties() {}
}
