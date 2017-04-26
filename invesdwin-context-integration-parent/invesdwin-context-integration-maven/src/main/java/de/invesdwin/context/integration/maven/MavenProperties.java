package de.invesdwin.context.integration.maven;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.sonatype.aether.repository.Authentication;
import org.sonatype.aether.repository.RemoteRepository;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.util.lang.Strings;

@Immutable
public final class MavenProperties {

    public static final File LOCAL_REPOSITORY_DIRECTORY;
    public static final List<RemoteRepository> REMOTE_REPOSITORIES;

    static {
        final SystemProperties systemProperties = new SystemProperties(MavenProperties.class);
        LOCAL_REPOSITORY_DIRECTORY = systemProperties.getFile("LOCAL_REPOSITORY_DIRECTORY");

        final List<RemoteRepository> remoteRepositories = new ArrayList<RemoteRepository>();
        int index = 1;
        while (true) {
            final String urlKey = "REMOTE_REPOSITORY_" + index + "_URL";
            if (!systemProperties.containsValue(urlKey)) {
                break;
            }
            final String url = systemProperties.getString(urlKey);

            final String usernameKey = "REMOTE_REPOSITORY_" + index + "_USERNAME";
            final String username;
            if (systemProperties.containsValue(usernameKey)) {
                username = systemProperties.getString(usernameKey);
            } else {
                username = null;
            }
            final String passwordKey = "REMOTE_REPOSITORY_" + index + "_PASSWORD";
            final String password;
            if (systemProperties.containsValue(passwordKey)) {
                password = systemProperties.getString(passwordKey);
            } else {
                password = null;
            }
            final RemoteRepository repo = new RemoteRepository(String.valueOf(index), "default", url);
            if (Strings.isNotBlank(username) || Strings.isNotBlank(password)) {
                repo.setAuthentication(new Authentication(username, password));
            }
            remoteRepositories.add(repo);
            index++;
        }
        REMOTE_REPOSITORIES = Collections.unmodifiableList(remoteRepositories);
    }

    private MavenProperties() {}

}
