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
                final File file = newFile();
                final ClasspathResourceProcessor processor = new ClasspathResourceProcessor();

                final FileOutputStream fos = new FileOutputStream(file);
                try (JarOutputStream jarOut = newJarOutputStream(fos)) {
                    beforeProcess(jarOut);
                    processor.process(new MergedClasspathJarVisitor(jarOut, filter));
                    afterProcess(jarOut);
                }
                alreadyGenerated = file;
            }
            return new FileSystemResource(alreadyGenerated);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    protected File newFile() {
        return new File(ContextProperties.TEMP_DIRECTORY, getClass().getName() + "_" + filter.name() + ".jar");
    }

    protected JarOutputStream newJarOutputStream(final FileOutputStream fos) throws IOException {
        return new JarOutputStream(fos);
    }

    protected void beforeProcess(final JarOutputStream jarOut) throws IOException {}

    protected void afterProcess(final JarOutputStream jarOut) throws IOException {}
}
