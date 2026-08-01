package de.invesdwin.context.integration.jar;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.visitor.IMergedClasspathJarFilter;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarVisitor;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.ClasspathResourceProcessor;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.time.Instant;

@ThreadSafe
public class MergedClasspathJar {

    private final Log log = new Log(this);

    private final IMergedClasspathJarFilter filter;
    @GuardedBy("this")
    private File alreadyGenerated;
    private final Class<?> mainClass;

    public MergedClasspathJar() {
        this(MergedClasspathJarFilter.DEFAULT);
    }

    public MergedClasspathJar(final IMergedClasspathJarFilter filter) {
        this(filter, null);
    }

    public MergedClasspathJar(final IMergedClasspathJarFilter filter, final Class<?> mainClass) {
        this.filter = filter;
        this.mainClass = mainClass;
    }

    public synchronized Resource getResource() {
        try {
            if (alreadyGenerated == null || !alreadyGenerated.exists()) {
                final File file = newFile();
                generate(file);
                alreadyGenerated = file;
            }
            return new FileSystemResource(alreadyGenerated);
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    protected void generate(final File file) throws FileNotFoundException, IOException {
        final Instant start = new Instant();
        log.info("Started generating [%s]", file);
        final ClasspathResourceProcessor processor = new ClasspathResourceProcessor();
        final FileOutputStream fos = new FileOutputStream(file);
        try (JarOutputStream jarOut = newJarOutputStream(fos)) {
            beforeProcess(jarOut);
            processor.process(new MergedClasspathJarVisitor(jarOut, filter));
            afterProcess(jarOut);
        }
        log.info("Finished generating [%s] after %s", file, start);
    }

    protected File newFile() {
        if (mainClass != null) {
            return new File(newFolder(), getClass().getSimpleName() + "_" + filter.name() + "_"
                    + mainClass.getSimpleName() + "_" + UUIDs.newPseudoRandomUUID() + ".jar");
        } else {
            return new File(newFolder(),
                    getClass().getSimpleName() + "_" + filter.name() + "_" + UUIDs.newPseudoRandomUUID() + ".jar");
        }
    }

    protected File newFolder() {
        return ContextProperties.TEMP_DIRECTORY;
    }

    protected JarOutputStream newJarOutputStream(final FileOutputStream fos) throws IOException {
        if (mainClass != null) {
            final Manifest manifest = new Manifest();
            final Attributes global = manifest.getMainAttributes();
            global.put(Attributes.Name.MANIFEST_VERSION, "1.0");
            global.put(Attributes.Name.MAIN_CLASS, mainClass.getName());
            return new JarOutputStream(fos, manifest);
        } else {
            return new JarOutputStream(fos);
        }
    }

    protected void beforeProcess(final JarOutputStream jarOut) throws IOException {}

    protected void afterProcess(final JarOutputStream jarOut) throws IOException {}
}
