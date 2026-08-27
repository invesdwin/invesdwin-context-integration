package de.invesdwin.context.integration.grid.hadoop.filechannel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPath;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPaths;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.time.date.FDate;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

@NotThreadSafe
public class HadoopFileChannel implements IFileChannel {

    public static final String DEFAULT_SERVER_URI_STR = "hdfs:///";
    public static final URI DEFAULT_SERVER_URI = URI.create(DEFAULT_SERVER_URI_STR);
    public static final Supplier<URI> DEFAULT_SERVER_URI_F = () -> DEFAULT_SERVER_URI;
    private static final boolean CACHED_FILE_SYSTEM = true;

    private static Supplier<Configuration> defaultConfigurationFactory = () -> new Configuration();

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;

    private transient Configuration configuration;

    @GuardedBy("this")
    private transient HadoopFileChannelFinalizer finalizer;

    private String subDirectory = "";
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private boolean directoryCreated = false;

    public HadoopFileChannel() {
        this(DEFAULT_SERVER_URI, defaultConfigurationFactory.get());
    }

    public HadoopFileChannel(final URI serverUri) {
        this(serverUri != null ? serverUri : DEFAULT_SERVER_URI, defaultConfigurationFactory.get());
    }

    public HadoopFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri), defaultConfigurationFactory.get());
    }

    public HadoopFileChannel(final String serverUri, final Configuration configuration) {
        this(serverUri == null ? null : URIs.asUri(serverUri), configuration);
    }

    public HadoopFileChannel(final URI serverUri, final Configuration configuration) {
        this.configuration = configuration != null ? configuration : defaultConfigurationFactory.get();
        final FileChannelPath path = FileChannelPath.valueOf(serverUri, DEFAULT_SERVER_URI_F);
        this.serverUri = path.getServerUri();
        this.baseServerUri = path.getBaseServerUri();
        this.baseDirectory = path.getAbsoluteDirectory();
        this.filename = path.getFilename();
    }

    public static Supplier<Configuration> getDefaultConfigurationFactory() {
        return defaultConfigurationFactory;
    }

    public static void setDefaultConfigurationFactory(final Supplier<Configuration> configurationFactory) {
        if (configurationFactory == null) {
            defaultConfigurationFactory = () -> new Configuration();
        } else {
            defaultConfigurationFactory = configurationFactory;
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public HadoopFileChannel setConfiguration(final Configuration configuration) {
        this.configuration = configuration != null ? configuration : defaultConfigurationFactory.get();
        if (finalizer != null && finalizer.fs != null) {
            try {
                if (!CACHED_FILE_SYSTEM) {
                    finalizer.fs.close();
                }
            } catch (final IOException e) {
                // Ignore close exceptions
            } finally {
                finalizer.fs = null;
            }
        }
        return this;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withSubDirectory(final String subDirectory) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.filename = filename;
        instance.setSubDirectory(subDirectory);
        return instance;
    }

    //CHECKSTYLE:OFF
    public HadoopFileChannel withConfiguration(final Configuration configuration) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.subDirectory = subDirectory;
        if (filename != null) {
            instance.setFilename(filename);
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withBaseServerUri(final URI baseServerUri) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(baseServerUri, getBaseDirectory());
        //CHECKSTYLE:OFF
        final HadoopFileChannel instance = new HadoopFileChannel(newServerUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withBaseServerUri(final String baseServerUri) {
        //CHECKSTYLE:ON
        return withBaseServerUri(URIs.asUri(baseServerUri));
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withBaseDirectory(final String baseDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(getBaseServerUri(), baseDirectory);
        //CHECKSTYLE:OFF
        final HadoopFileChannel instance = new HadoopFileChannel(newServerUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withAbsoluteDirectory(final String absoluteDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(getBaseServerUri(), absoluteDirectory);
        //CHECKSTYLE:OFF
        final HadoopFileChannel instance = new HadoopFileChannel(newServerUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withSubPath(final String subPath) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(subPath);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withSubPath(final java.nio.file.Path path) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(path);
        return instance;
    }

    //CHECKSTYLE:OFF
    public HadoopFileChannel withSubPath(final Path path) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(path);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withFilename(final String filename) {
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.configuration);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        instance.setFilename(filename);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withAbsolutePath(final String path) {
        //CHECKSTYLE:ON
        if (Strings.isBlank(path)) {
            //CHECKSTYLE:OFF
            final HadoopFileChannel instance = new HadoopFileChannel(getBaseServerUri(), this.configuration);
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath((String) null);
            return instance;
        }
        if (path.contains("://")) {
            final IFileChannel registryChannel = FileChannelRegistry.newInstance(path);
            if (registryChannel instanceof HadoopFileChannel) {
                return ((HadoopFileChannel) registryChannel).withConfiguration(this.configuration);
            }
            return (HadoopFileChannel) registryChannel;
        } else {
            //CHECKSTYLE:OFF
            final HadoopFileChannel instance = new HadoopFileChannel(getBaseServerUri(), this.configuration);
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath(path);
            return instance;
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public HadoopFileChannel withAbsolutePath(final java.nio.file.Path path) {
        //CHECKSTYLE:ON
        return withAbsolutePath(path != null ? path.toString() : null);
    }

    //CHECKSTYLE:OFF
    public HadoopFileChannel withAbsolutePath(final Path path) {
        //CHECKSTYLE:ON
        if (path == null) {
            return withAbsolutePath((String) null);
        }
        final URI uri = path.toUri();
        if (uri.getScheme() != null && !uri.getScheme().equalsIgnoreCase("hdfs")
                && !uri.getScheme().equalsIgnoreCase("viewfs")) {
            return withAbsolutePath(uri.getPath() != null ? uri.getPath() : path.toString());
        }
        return withAbsolutePath(uri.toString());
    }

    @Override
    public URI getServerUri() {
        return serverUri;
    }

    public URI getServerUriObject() {
        return serverUri;
    }

    @Override
    public URI getBaseServerUri() {
        return baseServerUri;
    }

    @Override
    public String getBaseDirectory() {
        return baseDirectory;
    }

    @Override
    public String getSubDirectory() {
        return subDirectory;
    }

    @Override
    public HadoopFileChannel setSubDirectory(final String subDirectory) {
        final String newSubDirectory = subDirectory != null ? subDirectory : "";
        if (!this.subDirectory.equals(newSubDirectory)) {
            this.subDirectory = newSubDirectory;
            this.directoryCreated = false;
        }
        return this;
    }

    @Override
    public HadoopFileChannel setFilename(final String filename) {
        this.filename = filename;
        return this;
    }

    @Override
    public HadoopFileChannel setSubPath(final String path) {
        IFileChannel.super.setSubPath(path);
        return this;
    }

    @Override
    public HadoopFileChannel setSubPath(final java.nio.file.Path path) {
        IFileChannel.super.setSubPath(path);
        return this;
    }

    public HadoopFileChannel setSubPath(final Path path) {
        if (path == null) {
            return setSubPath((String) null);
        }
        return setSubPath(path.toUri().getPath());
    }

    @Override
    public String getFilename() {
        return filename;
    }

    @Override
    public byte[] getEmptyFileContent() {
        return emptyFileContent;
    }

    @Override
    public HadoopFileChannel setEmptyFileContent(final byte[] emptyFileContent) {
        this.emptyFileContent = emptyFileContent;
        return this;
    }

    @Override
    public HadoopFileChannel createUniqueFile() {
        return createUniqueFile(HadoopFileChannel.class.getSimpleName() + "_", ".channel");
    }

    @Override
    public HadoopFileChannel createUniqueFile(final String filenamePrefix, final String filenameSuffix) {
        ensureDirectoryCreated();
        while (true) {
            final String filename = filenamePrefix + UUIDs.newPseudoRandomUUID() + filenameSuffix;
            setFilename(filename);
            if (!exists()) {
                upload(new FastByteArrayInputStream(getEmptyFileContent()));
                Assertions.checkTrue(exists());
                break;
            }
        }
        return this;
    }

    @Override
    public HadoopFileChannel connect() {
        return connect(true);
    }

    @Override
    public HadoopFileChannel connect(final boolean createDirectory) {
        try {
            if (finalizer == null) {
                finalizer = new HadoopFileChannelFinalizer();
            }
            if (finalizer.fs == null) {
                if (CACHED_FILE_SYSTEM) {
                    finalizer.fs = FileSystem.get(serverUri, configuration);
                } else {
                    finalizer.fs = FileSystem.newInstance(serverUri, configuration);
                }
                finalizer.register(this);
            }
            if (createDirectory && !directoryCreated) {
                createDirectory();
            }
            return this;
        } catch (final Throwable e) {
            close();
            throw new RuntimeException("Failed to connect to Hadoop FileSystem at " + serverUri, e);
        }
    }

    @Override
    public boolean isConnected() {
        return finalizer != null && finalizer.fs != null;
    }

    @Override
    public boolean exists() {
        connect(false);
        try {
            if (filename == null) {
                return finalizer.fs.exists(resolveDirectoryPath());
            }
            return finalizer.fs.exists(resolveFilePath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long length() {
        connect(false);
        try {
            final Path path = resolveFilePath();
            if (finalizer.fs.exists(path)) {
                return finalizer.fs.getFileStatus(path).getLen();
            }
            return -1;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FDate lastModified() {
        connect(false);
        try {
            final Path path = resolveFilePath();
            if (finalizer.fs.exists(path)) {
                return new FDate(finalizer.fs.getFileStatus(path).getModificationTime());
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileInfo info() {
        connect(false);
        try {
            final Path path = resolveFilePath();
            if (finalizer.fs.exists(path)) {
                return HadoopFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory,
                        finalizer.fs.getFileStatus(path));
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<HadoopFileInfo> list() {
        connect(false);
        try {
            final Path dirPath = resolveDirectoryPath();
            if (!finalizer.fs.exists(dirPath)) {
                return java.util.Collections.emptyList();
            }

            final FileStatus[] statuses = finalizer.fs.listStatus(dirPath);
            if (statuses == null) {
                return java.util.Collections.emptyList();
            }

            return Arrays.stream(statuses)
                    .map(status -> HadoopFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory,
                            status))
                    .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<HadoopFileInfo> listFiles() {
        return (List<HadoopFileInfo>) IFileChannel.super.listFiles();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<HadoopFileInfo> listDirectories() {
        return (List<HadoopFileInfo>) IFileChannel.super.listDirectories();
    }

    private void ensureDirectoryCreated() {
        connect(false);
        if (!directoryCreated) {
            createDirectory();
        }
    }

    @Override
    public HadoopFileChannel createDirectory() {
        connect(false);
        try {
            final Path dirPath = resolveDirectoryPath();
            if (!finalizer.fs.exists(dirPath)) {
                finalizer.fs.mkdirs(dirPath);
            }
            directoryCreated = true;
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create Hadoop directory structure at " + resolveDirectoryPath(), e);
        }
        return this;
    }

    private Path resolveDirectoryPath() {
        return new Path(FileChannelPaths.newDirectoryUri(baseServerUri, getAbsoluteDirectory()));
    }

    private Path resolveFilePath() {
        return new Path(FileChannelPaths.newFileUri(baseServerUri, getAbsoluteDirectory(), getFilename()));
    }

    @Override
    public HadoopFileChannel rename(final String filename) {
        connect(false);
        try {
            final Path source = resolveFilePath();
            final Path target = new Path(FileChannelPaths.newFileUri(baseServerUri, getAbsoluteDirectory(), filename));

            if (!finalizer.fs.rename(source, target)) {
                throw new RuntimeException("Hadoop rename operation returned false from " + source + " to " + target);
            }
            setFilename(filename);
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    @Override
    public void moveSameType(final IFileChannel targetChannel) {
        connect(false);
        try {
            final HadoopFileChannel targetHadoop = (HadoopFileChannel) targetChannel;
            targetHadoop.ensureDirectoryCreated();
            final Path source = resolveFilePath();
            final Path target = targetHadoop.resolveFilePath();
            if (!finalizer.fs.rename(source, target)) {
                throw new RuntimeException("Hadoop move operation returned false from " + source + " to " + target);
            }
            setSubDirectory(targetHadoop.getSubDirectory());
            setFilename(targetHadoop.getFilename());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileChannel upload(final File file) {
        ensureDirectoryCreated();
        try {
            finalizer.fs.copyFromLocalFile(false, true, new Path(file.toURI()), resolveFilePath());
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileChannel upload(final byte[] bytes) {
        return upload(new FastByteArrayInputStream(bytes));
    }

    @Override
    public HadoopFileChannel upload(final InputStream input) {
        ensureDirectoryCreated();
        try (FSDataOutputStream out = finalizer.fs.create(resolveFilePath(), true)) {
            IOUtils.copyLarge(input, out);
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.close(input);
        }
    }

    @Override
    public HadoopFileChannel uploadString(final String text) {
        IFileChannel.super.uploadString(text);
        return this;
    }

    @Override
    public HadoopFileChannel download(final File destination) {
        connect(false);
        try {
            final Path source = resolveFilePath();
            if (finalizer.fs.exists(source)) {
                Files.forceMkdirParent(destination);
                finalizer.fs.copyToLocalFile(false, source, new Path(destination.toURI()), true);
            }
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] downloadBytes() {
        connect(false);
        try {
            final Path source = resolveFilePath();
            if (!finalizer.fs.exists(source)) {
                return null;
            }

            final FileStatus status = finalizer.fs.getFileStatus(source);
            try (FSDataInputStream in = finalizer.fs.open(source)) {
                final byte[] data = new byte[(int) status.getLen()];
                in.readFully(data);
                return data;
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileChannel delete() {
        connect(false);
        try {
            final Path path = resolveFilePath();
            if (finalizer.fs.exists(path)) {
                finalizer.fs.delete(path, false);
            }
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (finalizer != null) {
            finalizer.close();
            finalizer = null;
        }
        directoryCreated = false;
    }

    @Override
    public OutputStream newUpload() {
        ensureDirectoryCreated();
        try {
            return finalizer.fs.create(resolveFilePath(), true);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream newDownload() {
        connect(false);
        try {
            final Path source = resolveFilePath();
            if (finalizer.fs.exists(source)) {
                return finalizer.fs.open(source);
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileChannel reconnect(final boolean createDirectory) {
        close();
        connect(createDirectory);
        return this;
    }

    @Override
    public String toString() {
        return FileChannelPaths.toString(this);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        if (configuration != null) {
            out.writeBoolean(true);
            configuration.write(out);
        } else {
            out.writeBoolean(false);
        }
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (in.readBoolean()) {
            configuration = new Configuration(false);
            configuration.readFields(in);
        } else {
            configuration = defaultConfigurationFactory.get();
        }
    }

    private static final class HadoopFileChannelFinalizer extends AFinalizer {

        private FileSystem fs;

        @Override
        protected void clean() {
            if (fs != null) {
                if (!CACHED_FILE_SYSTEM) {
                    try {
                        fs.close();
                    } catch (final IOException e) {
                        // ignore
                    }
                }
                fs = null;
            }
        }

        @Override
        protected boolean isCleaned() {
            return fs == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}