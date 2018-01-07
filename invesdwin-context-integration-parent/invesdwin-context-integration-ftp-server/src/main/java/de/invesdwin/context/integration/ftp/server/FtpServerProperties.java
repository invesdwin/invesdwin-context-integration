package de.invesdwin.context.integration.ftp.server;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.uri.URIs;

@Immutable
public final class FtpServerProperties {

    public static final Integer PORT;
    public static final int MAX_THREADS;;

    private static final String KEY_MAX_THREADS = "MAX_THREADS";

    static {
        final SystemProperties systemProperties = new SystemProperties();
        PORT = systemProperties.getPort("PORT", true);
        if (systemProperties.containsValue(KEY_MAX_THREADS)) {
            MAX_THREADS = systemProperties.getInteger(KEY_MAX_THREADS);
        } else {
            MAX_THREADS = Runtime.getRuntime().availableProcessors();
            systemProperties.setInteger(KEY_MAX_THREADS, MAX_THREADS);
        }
    }

    private FtpServerProperties() {}

    public static URI getServerBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + PORT);
    }

}
