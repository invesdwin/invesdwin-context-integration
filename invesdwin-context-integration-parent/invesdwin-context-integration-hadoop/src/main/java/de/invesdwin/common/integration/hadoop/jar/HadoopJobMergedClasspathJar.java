package de.invesdwin.common.integration.hadoop.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import de.invesdwin.common.integration.hadoop.jar.internal.HadoopJobMergedClasspathJarVisitor;
import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.ClasspathResourceProcessor;

@ThreadSafe
public class HadoopJobMergedClasspathJar {

    @GuardedBy("this.class")
    private static File alreadyGenerated;

    public Resource getResource() {
        synchronized (HadoopJobMergedClasspathJar.class) {
            try {
                if (alreadyGenerated == null || !alreadyGenerated.exists()) {
                    final File file = new File(ContextProperties.TEMP_CLASSPATH_DIRECTORY,
                            HadoopJobMergedClasspathJar.class.getName() + ".jar");
                    final ClasspathResourceProcessor processor = new ClasspathResourceProcessor();

                    try (final JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(file))) {
                        processor.process(new HadoopJobMergedClasspathJarVisitor(jarOut));
                    }
                    alreadyGenerated = file;
                }
                return new FileSystemResource(alreadyGenerated);
            } catch (final IOException e) {
                throw Err.process(e);
            }
        }
    }
}
