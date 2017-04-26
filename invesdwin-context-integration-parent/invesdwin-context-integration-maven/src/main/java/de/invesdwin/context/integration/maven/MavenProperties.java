package de.invesdwin.context.integration.maven;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import javax.annotation.concurrent.Immutable;

import org.sonatype.aether.repository.Authentication;
import org.sonatype.aether.repository.RemoteRepository;

import com.jcabi.aether.Aether;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Strings;

@Immutable
public final class MavenProperties {

    public static final File LOCAL_REPOSITORY_DIRECTORY;
    public static final URL DEFAULT_REMOTE_REPOSITORY_URL;
    public static final String DEFAULT_REMOTE_REPOSITORY_USERNAME;
    public static final String DEFAULT_REMOTE_REPOSITORY_PASSWORD;

    static {
        final SystemProperties systemProperties = new SystemProperties(MavenProperties.class);
        LOCAL_REPOSITORY_DIRECTORY = systemProperties.getFile("LOCAL_REPOSITORY_DIRECTORY");
        DEFAULT_REMOTE_REPOSITORY_URL = systemProperties.getURL("DEFAULT_REMOTE_REPOSITORY_URL", false);
        final String usernameKey = "DEFAULT_REMOTE_REPOSITORY_USERNAME";
        if (systemProperties.containsValue(usernameKey)) {
            DEFAULT_REMOTE_REPOSITORY_USERNAME = systemProperties.getString(usernameKey);
        } else {
            DEFAULT_REMOTE_REPOSITORY_USERNAME = null;
        }
        final String passwordKey = "DEFAULT_REMOTE_REPOSITORY_PASSWORD";
        if (systemProperties.containsValue(passwordKey)) {
            DEFAULT_REMOTE_REPOSITORY_PASSWORD = systemProperties.getString(passwordKey);
        } else {
            DEFAULT_REMOTE_REPOSITORY_PASSWORD = null;
        }
    }

    private MavenProperties() {}

    public static Authentication newDefaultAuthentication() {
        if (Strings.isNotBlank(DEFAULT_REMOTE_REPOSITORY_USERNAME)
                || Strings.isNotBlank(DEFAULT_REMOTE_REPOSITORY_PASSWORD)) {
            return new Authentication(DEFAULT_REMOTE_REPOSITORY_USERNAME, DEFAULT_REMOTE_REPOSITORY_PASSWORD);
        } else {
            return null;
        }
    }

    public static Aether newDefaultAether() {
        final RemoteRepository remoteRepository = new RemoteRepository("default", "default",
                DEFAULT_REMOTE_REPOSITORY_URL.toString());
        remoteRepository.setAuthentication(newDefaultAuthentication());
        final Collection<RemoteRepository> remotes = Arrays.asList(remoteRepository);
        final Aether aether = new Aether(remotes, LOCAL_REPOSITORY_DIRECTORY);
        return aether;
    }

}
