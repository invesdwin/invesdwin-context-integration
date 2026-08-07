package de.invesdwin.context.integration.grid.hadoop.filechannel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.UUIDs;
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

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;

    // Marked as transient since Configuration is not Serializable
    private transient Configuration conf;
    private transient FileSystem fs;

    private String subDirectory = "";
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private boolean connected = false;

    public HadoopFileChannel() {
        this(DEFAULT_SERVER_URI, new Configuration());
    }

    public HadoopFileChannel(final URI serverUri) {
        this(serverUri != null ? serverUri : DEFAULT_SERVER_URI, new Configuration());
    }

    public HadoopFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri), new Configuration());
    }

    public HadoopFileChannel(final String serverUri, final Configuration conf) {
        this(serverUri == null ? null : URIs.asUri(serverUri), conf);
    }

    public HadoopFileChannel(final URI serverUri, final Configuration conf) {
        this.conf = conf != null ? conf : new Configuration();
        this.serverUri = serverUri != null ? serverUri : DEFAULT_SERVER_URI;
        this.baseServerUri = FileChannelInfos.extractBaseServerUri(this.serverUri, DEFAULT_SERVER_URI);
        this.baseDirectory = FileChannelInfos.extractBaseDirectory(this.serverUri);
    }

    public static String combinePath(final String baseDirectory, final String subDirectory) {
        if (Strings.isBlank(subDirectory)) {
            return baseDirectory;
        }
        String cleanDir = subDirectory.replace("\\", "/").replaceAll("[/]+", "/");
        while (cleanDir.startsWith("/")) {
            cleanDir = cleanDir.substring(1);
        }
        if (cleanDir.isEmpty()) {
            return baseDirectory;
        }
        return Strings.putSuffix(baseDirectory + cleanDir, "/");
    }

    //CHECKSTYLE:OFF
    @Override
    public IFileChannel withSubDirectory(final String subDirectory) {
        //CHECKSTYLE:ON
        final HadoopFileChannel instance = new HadoopFileChannel(serverUri, this.conf);
        instance.emptyFileContent = emptyFileContent;
        instance.filename = filename;
        instance.setSubDirectory(subDirectory);
        return instance;
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
    public String getAbsoluteDirectory() {
        return combinePath(baseDirectory, subDirectory);
    }

    @Override
    public HadoopFileChannel setSubDirectory(final String subDirectory) {
        this.subDirectory = subDirectory != null ? subDirectory : "";
        createDirectoryIfNotExists();
        return this;
    }

    @Override
    public HadoopFileChannel setFilename(final String filename) {
        this.filename = filename;
        return this;
    }

    @Override
    public String getFilename() {
        if (filename == null) {
            throw new NullPointerException("please call setFilename(...) first");
        }
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
        assertConnected();
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
        try {
            if (fs == null) {
                // Using newInstance prevents closing a cached FileSystem shared by the JVM
                fs = FileSystem.newInstance(serverUri, conf);
            }
            connected = true;
            createDirectoryIfNotExists();
            return this;
        } catch (final IOException e) {
            throw new RuntimeException("Failed to connect to Hadoop FileSystem at " + serverUri, e);
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean exists() {
        assertConnected();
        try {
            return fs.exists(resolveFilePath());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long length() {
        assertConnected();
        try {
            final Path path = resolveFilePath();
            if (fs.exists(path)) {
                return fs.getFileStatus(path).getLen();
            }
            return -1;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FDate lastModified() {
        assertConnected();
        try {
            final Path path = resolveFilePath();
            if (fs.exists(path)) {
                return new FDate(fs.getFileStatus(path).getModificationTime());
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileInfo info() {
        assertConnected();
        try {
            final Path path = resolveFilePath();
            if (fs.exists(path)) {
                return HadoopFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory,
                        fs.getFileStatus(path));
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<HadoopFileInfo> list() {
        assertConnected();
        try {
            final Path dirPath = resolveDirectoryPath();
            if (!fs.exists(dirPath)) {
                return java.util.Collections.emptyList();
            }

            final FileStatus[] statuses = fs.listStatus(dirPath);
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

    private void assertConnected() {
        Assertions.checkTrue(isConnected(), "Please call connect() first");
    }

    private void createDirectoryIfNotExists() {
        if (isConnected()) {
            try {
                final Path dirPath = resolveDirectoryPath();
                if (!fs.exists(dirPath)) {
                    fs.mkdirs(dirPath);
                }
            } catch (final IOException e) {
                throw new RuntimeException("Failed to create Hadoop directory structure at " + resolveDirectoryPath(),
                        e);
            }
        }
    }

    private Path resolveDirectoryPath() {
        return new Path(FileChannelInfos.newDirectoryUri(baseServerUri, getAbsoluteDirectory()));
    }

    private Path resolveFilePath() {
        return new Path(FileChannelInfos.newFileUri(baseServerUri, getAbsoluteDirectory(), getFilename()));
    }

    @Override
    public HadoopFileChannel rename(final String filename) {
        assertConnected();
        try {
            final Path source = resolveFilePath();
            final Path target = new Path(FileChannelInfos.newFileUri(baseServerUri, getAbsoluteDirectory(), filename));

            if (!fs.rename(source, target)) {
                throw new RuntimeException("Hadoop rename operation returned false from " + source + " to " + target);
            }
            setFilename(filename);
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HadoopFileChannel upload(final File file) {
        assertConnected();
        try {
            fs.copyFromLocalFile(false, true, new Path(file.toURI()), resolveFilePath());
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
        assertConnected();
        try (FSDataOutputStream out = fs.create(resolveFilePath(), true)) {
            copyStream(input, out);
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.close(input);
        }
    }

    @Override
    public HadoopFileChannel download(final File destination) {
        assertConnected();
        try {
            final Path source = resolveFilePath();
            if (fs.exists(source)) {
                Files.forceMkdirParent(destination);
                fs.copyToLocalFile(false, source, new Path(destination.toURI()), true);
            }
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] download() {
        assertConnected();
        try {
            final Path source = resolveFilePath();
            if (!fs.exists(source)) {
                return null;
            }

            final FileStatus status = fs.getFileStatus(source);
            try (FSDataInputStream in = fs.open(source)) {
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
        assertConnected();
        try {
            final Path path = resolveFilePath();
            if (fs.exists(path)) {
                fs.delete(path, false);
            }
            return this;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        connected = false;
        if (fs != null) {
            try {
                fs.close();
            } catch (final IOException e) {
                // Ignore close exceptions
            } finally {
                fs = null;
            }
        }
    }

    @Override
    public OutputStream uploadOutputStream() {
        assertConnected();
        try {
            return fs.create(resolveFilePath(), true);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream downloadInputStream() {
        assertConnected();
        try {
            final Path source = resolveFilePath();
            if (fs.exists(source)) {
                return fs.open(source);
            }
            return null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public File getLocalTempFile() {
        final File directory = new File(ContextProperties.TEMP_DIRECTORY, getAbsoluteDirectory());
        try {
            Files.forceMkdir(directory);
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }

        final File file = new File(directory, getFilename());
        Files.deleteQuietly(file);

        if (exists()) {
            download(file);
        }

        return file;
    }

    @Override
    public HadoopFileChannel reconnect() {
        close();
        connect();
        return this;
    }

    @Override
    public String toString() {
        return FileChannelInfos.toString(this);
    }

    private void copyStream(final InputStream in, final OutputStream out) throws IOException {
        final byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
    }

    /**
     * Custom serialization method to save the Hadoop Configuration state.
     */
    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        if (conf != null) {
            out.writeBoolean(true);
            conf.write(out);
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Custom deserialization method to load the Hadoop Configuration state.
     */
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (in.readBoolean()) {
            conf = new Configuration();
            conf.readFields(in);
        } else {
            conf = new Configuration();
        }
    }
}