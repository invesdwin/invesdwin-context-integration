package de.invesdwin.context.integration.grid.jar.upload;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.shutdown.IShutdownHook;
import de.invesdwin.util.shutdown.ShutdownHookManager;
import de.invesdwin.util.streams.closeable.ISafeCloseable;

@ThreadSafe
public class UploadedMergedClasspathJar implements ISafeCloseable {

    private static final Log LOG = new Log(UploadedMergedClasspathJar.class);

    private final String fileChannelSubDirectory;
    private File mergedClasspathJarFileUploaded;
    private File mergedClasspathJarFile;
    private UploadedMergedClasspathJarShutdownHook shutdownHook;

    public UploadedMergedClasspathJar(final String fileChannelSubDirectory) {
        this.fileChannelSubDirectory = fileChannelSubDirectory;
    }

    /**
     * WARNING: we should only use SSL encrypted webdav to share classpath jars, otherwise an MITM attack could fetch
     * the code or inject malicious code.
     */
    public File getMergedClasspathJarFileUploaded(final URI filechannelServerUri) {
        if (mergedClasspathJarFileUploaded == null) {
            synchronized (this) {
                if (mergedClasspathJarFileUploaded == null) {
                    mergedClasspathJarFileUploaded = uploadClasspathJar(filechannelServerUri);
                }
            }
        }
        return mergedClasspathJarFileUploaded;
    }

    private File uploadClasspathJar(final URI filechannelServerUri) {
        try (IFileChannel channel = FileChannelRegistry.newInstance(filechannelServerUri)
                .setSubDirectory(fileChannelSubDirectory)) {
            try {
                final File mergedClasspathJarFile = getMergedClasspathJarFile();
                channel.setFilename(mergedClasspathJarFile.getName());
                channel.upload(mergedClasspathJarFile);
                this.shutdownHook = new UploadedMergedClasspathJarShutdownHook(filechannelServerUri);
                ShutdownHookManager.register(shutdownHook);
                LOG.info("Successfully uploaded merged classpath JAR [%s] to [%s]", mergedClasspathJarFile.getName(),
                        channel);
                return mergedClasspathJarFile;
            } catch (final Throwable t) {
                throw Err
                        .process(new RuntimeException("Failed to upload merged classpath JAR to [" + channel + "]", t));
            }
        }
    }

    public File getMergedClasspathJarFile() throws IOException {
        if (mergedClasspathJarFile == null) {
            synchronized (this) {
                if (mergedClasspathJarFile == null) {
                    mergedClasspathJarFile = newMergedClasspathJar().getResource().getFile();
                }
            }
        }
        return mergedClasspathJarFile;
    }

    protected MergedClasspathJar newMergedClasspathJar() {
        return new MergedClasspathJar();
    }

    private void maybeDeleteClasspathJar(final URI filechannelServerUri) {
        if (mergedClasspathJarFileUploaded != null) {
            synchronized (this) {
                final File mergedClasspathJarFileUploadedCopy = mergedClasspathJarFileUploaded;
                if (mergedClasspathJarFileUploadedCopy != null) {
                    try (IFileChannel channel = FileChannelRegistry.newInstance(filechannelServerUri)
                            .setSubDirectory(fileChannelSubDirectory)
                            .setFilename(mergedClasspathJarFileUploadedCopy.getName())) {
                        channel.delete();
                        mergedClasspathJarFileUploaded = null;
                        LOG.info("Successfully deleted merged classpath JAR [%s] from [%s]",
                                mergedClasspathJarFileUploadedCopy.getName(),
                                FileChannelInfos.newFileUri(filechannelServerUri, fileChannelSubDirectory,
                                        mergedClasspathJarFileUploadedCopy.getName()));
                    } catch (final Throwable t) {
                        throw Err.process(new RuntimeException("Failed to delete merged classpath JAR from ["
                                + FileChannelInfos.newFileUri(filechannelServerUri, fileChannelSubDirectory,
                                        mergedClasspathJarFileUploadedCopy.getName())
                                + "]", t));
                    }
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        IShutdownHook shutdownHookCopy = shutdownHook;
        if (shutdownHookCopy != null) {
            try {
                shutdownHookCopy.shutdown();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            shutdownHookCopy = null;
            shutdownHook = null;
        }
    }

    private final class UploadedMergedClasspathJarShutdownHook implements IShutdownHook {
        private final URI filechannelServerUri;

        private UploadedMergedClasspathJarShutdownHook(final URI filechannelServerUri) {
            this.filechannelServerUri = filechannelServerUri;
        }

        @Override
        public void shutdown() throws Exception {
            maybeDeleteClasspathJar(filechannelServerUri);
        }
    }

}
