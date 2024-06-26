package de.invesdwin.context.integration.maven;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import org.jboss.shrinkwrap.resolver.api.maven.ConfigurableMavenResolverSystem;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class MavenPropertiesTest extends ATest {

    @Test
    public void testResolve() {
        final ConfigurableMavenResolverSystem resolver = MavenProperties.newResolver();
        final File[] deps = resolver.resolve("junit:junit-dep:jar:4.10").withoutTransitivity().asFile();
        for (final File dep : deps) {
            log.info("%s", dep);
        }
        Assertions.checkNotEmpty(deps);
    }

}
