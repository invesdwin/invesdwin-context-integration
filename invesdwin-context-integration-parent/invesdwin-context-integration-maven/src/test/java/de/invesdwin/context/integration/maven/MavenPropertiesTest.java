package de.invesdwin.context.integration.maven;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.util.artifact.DefaultArtifact;

import com.jcabi.aether.Aether;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class MavenPropertiesTest extends ATest {

    @Test
    public void testResolve() throws DependencyResolutionException {
        final Aether aether = new Aether(MavenProperties.REMOTE_REPOSITORIES,
                MavenProperties.LOCAL_REPOSITORY_DIRECTORY);
        final DefaultArtifact artifact = new DefaultArtifact("junit", "junit-dep", "", "jar", "4.10");
        final List<Artifact> deps = aether.resolve(artifact, "runtime");
        for (final Artifact dep : deps) {
            log.info("%s", dep.getFile());
        }
        Assertions.checkNotEmpty(deps);
    }

}
