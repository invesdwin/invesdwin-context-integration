package de.invesdwin.context.integration.maven;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import org.jboss.shrinkwrap.resolver.api.maven.repository.MavenRemoteRepositories;
import org.jboss.shrinkwrap.resolver.api.maven.repository.MavenRemoteRepository;

import de.invesdwin.context.system.properties.SystemProperties;

@Immutable
public final class MavenProperties {

    public static final File LOCAL_REPOSITORY_DIRECTORY;
    public static final List<MavenRemoteRepository> REMOTE_REPOSITORIES;

    static {
        final SystemProperties systemProperties = new SystemProperties(MavenProperties.class);
        LOCAL_REPOSITORY_DIRECTORY = systemProperties.getFile("LOCAL_REPOSITORY_DIRECTORY");
        //tell shrinkwrap where the files should be resolved to
        new SystemProperties().setString("maven.repo.local", LOCAL_REPOSITORY_DIRECTORY.toString());

        final List<MavenRemoteRepository> remoteRepositories = new ArrayList<MavenRemoteRepository>();
        int index = 1;
        while (true) {
            final String urlKey = "REMOTE_REPOSITORY_" + index + "_URL";
            if (!systemProperties.containsValue(urlKey)) {
                break;
            }
            final String url = systemProperties.getString(urlKey);
            final MavenRemoteRepository repo = MavenRemoteRepositories.createRemoteRepository(String.valueOf(index),
                    url, "default");
            remoteRepositories.add(repo);
            index++;
        }
        REMOTE_REPOSITORIES = Collections.unmodifiableList(remoteRepositories);

    }

    private MavenProperties() {}

}
