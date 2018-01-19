package de.invesdwin.context.integration.ftp;

import java.io.File;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class FtpClientProperties {

    public static final File TEMP_DIRECTORY = new File(ContextProperties.TEMP_DIRECTORY,
            FtpFileChannel.class.getSimpleName());
    public static final Duration PURGE_TEMP_FILES_OLDER_THAN_DURATION = Duration.ONE_DAY;
    public static final String SERVICE_NAME = "invesdwin-context-integration-ftp-server";
    public static final String USERNAME;
    public static final String PASSWORD;
    public static final Duration SOCKET_TIMEOUT;

    static {
        final SystemProperties systemProperties = new SystemProperties(FtpClientProperties.class);
        USERNAME = systemProperties.getString("USERNAME");
        PASSWORD = systemProperties.getStringWithSecurityWarning("PASSWORD", "invesdwin");
        SOCKET_TIMEOUT = systemProperties.getDuration("SOCKET_TIMEOUT");
    }

    private FtpClientProperties() {}

}
