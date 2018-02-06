package de.invesdwin.context.integration.webdav.server;

import java.io.File;
import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.webdav.server.internal.ConfiguredWebdavServlet;
import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class WebdavServerProperties {

    public static final File WORKING_DIRECTORY;
    public static final Duration PURGE_FILES_OLDER_THAN_DURATION;

    private static final String KEY_PURGE_FILES_OLDER_THAN_DURATION = "PURGE_FILES_OLDER_THAN_DURATION";

    static {
        final SystemProperties systemProperties = new SystemProperties(WebdavServerProperties.class);
        if (systemProperties.containsValue(KEY_PURGE_FILES_OLDER_THAN_DURATION)) {
            PURGE_FILES_OLDER_THAN_DURATION = systemProperties.getDuration(KEY_PURGE_FILES_OLDER_THAN_DURATION);
        } else {
            PURGE_FILES_OLDER_THAN_DURATION = null;
        }
        WORKING_DIRECTORY = new File(ContextProperties.getCacheDirectory(),
                ConfiguredWebdavServlet.class.getSimpleName());
    }

    private WebdavServerProperties() {}

    public static URI getServerBindUri() {
        return URIs.asUri(IntegrationProperties.WEBSERVER_BIND_URI + "/webdav/");
    }

}
