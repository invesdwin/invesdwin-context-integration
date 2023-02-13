package de.invesdwin.context.integration.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.IMergedClasspathJarFilter;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarVisitor;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.ClasspathResourceProcessor;

@ThreadSafe
public class MergedClasspathJar {

    private final IMergedClasspathJarFilter filter;
    @GuardedBy("this")
    private File alreadyGenerated;

    public MergedClasspathJar() {
        this(MergedClasspathJarFilter.DEFAULT);
    }

    public MergedClasspathJar(final IMergedClasspathJarFilter filter) {
        this.filter = filter;
    }

    public synchronized Resource getResource() {
        try {
            if (alreadyGenerated == null || !alreadyGenerated.exists()) {
                final File file = new File(ContextProperties.TEMP_DIRECTORY,
                        MergedClasspathJar.class.getName() + ".jar");
                final ClasspathResourceProcessor processor = new ClasspathResourceProcessor();

                try (JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(file))) {
                    processor.process(new MergedClasspathJarVisitor(jarOut, filter));
                }
                alreadyGenerated = file;
            }
            return new FileSystemResource(alreadyGenerated);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }
}
