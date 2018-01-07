package de.invesdwin.context.integration.ftp.server;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.io.FileUtils;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class FtpServerProperties {

    public static final File BASE_DIRECTORY;
    public static final Integer PORT;
    public static final int MAX_THREADS;
    public static final Duration PURGE_FILES_OLDER_THAN_DURATION;

    private static final String KEY_MAX_THREADS = "MAX_THREADS";
    private static final String KEY_PURGE_FILES_OLDER_THAN_DURATION = "PURGE_FILES_OLDER_THAN_DURATION";

    static {
        final SystemProperties systemProperties = new SystemProperties(FtpServerProperties.class);
        PORT = systemProperties.getPort("PORT", true);
        if (systemProperties.containsValue(KEY_MAX_THREADS)) {
            MAX_THREADS = systemProperties.getInteger(KEY_MAX_THREADS);
        } else {
            MAX_THREADS = Runtime.getRuntime().availableProcessors();
            systemProperties.setInteger(KEY_MAX_THREADS, MAX_THREADS);
        }
        if (systemProperties.containsValue(KEY_PURGE_FILES_OLDER_THAN_DURATION)) {
            PURGE_FILES_OLDER_THAN_DURATION = systemProperties.getDuration(KEY_PURGE_FILES_OLDER_THAN_DURATION);
        } else {
            PURGE_FILES_OLDER_THAN_DURATION = null;
        }
        BASE_DIRECTORY = new File(ContextProperties.getCacheDirectory(), ConfiguredFtpServer.class.getSimpleName());
        try {
            FileUtils.forceMkdir(BASE_DIRECTORY);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FtpServerProperties() {}

    public static URI getServerBindUri() {
        return URIs.asUri("p://" + IntegrationProperties.HOSTNAME + ":" + PORT);
    }

}
