package de.invesdwin.context.integration.grid.jar.upload;

import java.io.File;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;

@ThreadSafe
public class DownloadedMergedClasspathJar {
    private static final Log LOG = new Log(DownloadedMergedClasspathJar.class);

    private final String fileChannelSubDirectory;

    public DownloadedMergedClasspathJar(final String fileChannelSubDirectory) {
        this.fileChannelSubDirectory = fileChannelSubDirectory;
    }

    public File getMergedClasspathJarFileDownloaded(final URI fileChannelServerUri, final String classpathJarName) {
        if (classpathJarName == null) {
            return null;
        }
        final File localClasspathJar = new File(
                new File(new File(ContextProperties.TEMP_DIRECTORY, DownloadedMergedClasspathJar.class.getSimpleName()),
                        fileChannelSubDirectory),
                classpathJarName);
        if (localClasspathJar.exists()) {
            return localClasspathJar;
        }
        synchronized (this) {
            if (localClasspathJar.exists()) {
                return localClasspathJar;
            }
            try (IFileChannel channel = FileChannelRegistry.newInstance(fileChannelServerUri)
                    .setSubDirectory(fileChannelSubDirectory)
                    .setFilename(classpathJarName)) {
                try {
                    if (channel.exists()) {
                        channel.download(localClasspathJar);
                        LOG.info("Downloaded merged classpath JAR from [%s] to remote node temp file: %s", channel,
                                localClasspathJar.getAbsolutePath());
                        return localClasspathJar;
                    }
                } catch (final Throwable t) {
                    throw Err.process(new RuntimeException(
                            "Failed to download merged classpath JAR [" + classpathJarName + "] from [" + channel + "]",
                            t));
                }
            }
            return null;
        }
    }

}
