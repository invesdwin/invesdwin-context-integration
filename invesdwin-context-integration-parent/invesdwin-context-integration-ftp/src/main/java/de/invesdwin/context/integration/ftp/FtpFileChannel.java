package de.invesdwin.context.integration.ftp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.DeletingFileInputStream;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.streams.delegate.ADelegateInputStream;
import de.invesdwin.util.streams.delegate.ADelegateOutputStream;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPCodes;
import it.sauronsoftware.ftp4j.FTPException;
import it.sauronsoftware.ftp4j.FTPFile;
import it.sauronsoftware.ftp4j.FTPIllegalReplyException;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

@ThreadSafe
public class FtpFileChannel implements IFileChannel {

    private static final Pattern MULTIPLE_SLASHES = Pattern.compile("[/]+");

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private String subDirectory = "";
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private boolean directoryCreated = false;

    @GuardedBy("this")
    private transient FtpFileChannelFinalizer finalizer;

    public FtpFileChannel(final URI serverUri) {
        if (serverUri == null) {
            throw new NullPointerException("serverUri should not be null");
        }
        this.serverUri = serverUri;
        this.baseServerUri = FileChannelInfos.extractBaseServerUri(this.serverUri, null);
        this.baseDirectory = FileChannelInfos.extractBaseDirectory(this.serverUri);
        this.filename = FileChannelInfos.extractFileName(serverUri);
    }

    public FtpFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri));
    }

    public static String combinePath(final String baseDirectory, final String subDirectory) {
        if (Strings.isBlank(subDirectory)) {
            return baseDirectory;
        }
        String cleanDir = MULTIPLE_SLASHES.matcher(subDirectory.replace("\\", "/")).replaceAll("/");
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
    public FtpFileChannel withSubDirectory(final String subDirectory) {
        final FtpFileChannel instance = new FtpFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.filename = filename;
        instance.setSubDirectory(subDirectory);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withBaseServerUri(final URI baseServerUri) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(baseServerUri, getBaseDirectory());
        //CHECKSTYLE:OFF
        final FtpFileChannel instance = new FtpFileChannel(newServerUri);
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
    public FtpFileChannel withBaseServerUri(final String baseServerUri) {
        //CHECKSTYLE:ON
        return withBaseServerUri(URIs.asUri(baseServerUri));
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withBaseDirectory(final String baseDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), baseDirectory);
        //CHECKSTYLE:OFF
        final FtpFileChannel instance = new FtpFileChannel(newServerUri);
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
    public FtpFileChannel withAbsoluteDirectory(final String absoluteDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), absoluteDirectory);
        //CHECKSTYLE:OFF
        final FtpFileChannel instance = new FtpFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withSubPath(final String subPath) {
        final FtpFileChannel instance = new FtpFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(subPath);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withSubPath(final Path path) {
        final FtpFileChannel instance = new FtpFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(path);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withFilename(final String filename) {
        final FtpFileChannel instance = new FtpFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        instance.setFilename(filename);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withAbsolutePath(final String path) {
        //CHECKSTYLE:ON
        if (Strings.isBlank(path)) {
            //CHECKSTYLE:OFF
            final FtpFileChannel instance = new FtpFileChannel(getBaseServerUri());
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath((String) null);
            return instance;
        }
        if (path.contains("://")) {
            return (FtpFileChannel) FileChannelRegistry.newInstance(path);
        } else {
            //CHECKSTYLE:OFF
            final FtpFileChannel instance = new FtpFileChannel(getBaseServerUri());
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath(path);
            return instance;
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public FtpFileChannel withAbsolutePath(final Path path) {
        //CHECKSTYLE:ON
        return withAbsolutePath(path != null ? path.toString() : null);
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
    public FtpFileChannel setSubDirectory(final String subDirectory) {
        final String newSubDirectory = subDirectory != null ? subDirectory : "";
        if (!this.subDirectory.equals(newSubDirectory)) {
            this.subDirectory = newSubDirectory;
            this.directoryCreated = false;
            if (isConnected()) {
                try {
                    finalizer.ftpClient.changeDirectory("/");
                    changeDirectoryOnly();
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return this;
    }

    @Override
    public FtpFileChannel setFilename(final String filename) {
        this.filename = filename;
        return this;
    }

    @Override
    public FtpFileChannel setSubPath(final String path) {
        IFileChannel.super.setSubPath(path);
        return this;
    }

    @Override
    public FtpFileChannel setSubPath(final Path path) {
        IFileChannel.super.setSubPath(path);
        return this;
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
    public FtpFileChannel setEmptyFileContent(final byte[] emptyFileContent) {
        this.emptyFileContent = emptyFileContent;
        return this;
    }

    @Override
    public FtpFileChannel createUniqueFile() {
        return createUniqueFile(FtpFileChannel.class.getSimpleName() + "_", ".channel");
    }

    @Override
    public FtpFileChannel createUniqueFile(final String filenamePrefix, final String filenameSuffix) {
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

    public FTPClient getFtpClient() {
        connect(false);
        return finalizer.ftpClient;
    }

    @Override
    public FtpFileChannel connect() {
        return connect(true);
    }

    @Override
    public FtpFileChannel connect(final boolean createDirectory) {
        try {
            if (finalizer == null) {
                finalizer = new FtpFileChannelFinalizer();
            }
            if (finalizer.ftpClient != null && finalizer.ftpClient.isConnected() && isAuthenticated()) {
                if (createDirectory && !directoryCreated) {
                    createDirectory();
                } else {
                    changeDirectoryOnly();
                }
                return this;
            }
            if (finalizer.ftpClient != null) {
                close();
                finalizer = new FtpFileChannelFinalizer();
            }
            finalizer.ftpClient = new FTPClient();
            //be a bit more firewall friendly
            finalizer.ftpClient.setPassive(true);

            final int timeoutSeconds = ContextProperties.DEFAULT_NETWORK_TIMEOUT.intValue(FTimeUnit.SECONDS);
            finalizer.ftpClient.setAutoNoopTimeout(timeoutSeconds * FTimeUnit.MILLISECONDS_IN_SECOND);
            finalizer.ftpClient.getConnector().setConnectionTimeout(timeoutSeconds);
            finalizer.ftpClient.getConnector().setReadTimeout(timeoutSeconds);
            finalizer.ftpClient.getConnector().setCloseTimeout(timeoutSeconds);
            finalizer.ftpClient.setType(FTPClient.TYPE_BINARY);
            finalizer.ftpClient.connect(serverUri.getHost(), serverUri.getPort());
            finalizer.register(this);
            login();
            if (createDirectory && !directoryCreated) {
                createDirectory();
            } else {
                changeDirectoryOnly();
            }
            return this;
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    protected void login() throws IOException, FTPIllegalReplyException, FTPException {
        finalizer.ftpClient.login(FtpClientProperties.USERNAME, FtpClientProperties.PASSWORD);
    }

    protected boolean isAuthenticated() {
        return finalizer.ftpClient.isAuthenticated();
    }

    private void createAndChangeDirectory() {
        final String absDir = getAbsoluteDirectory();
        final String[] pathElements = absDir.split("/");
        final StringBuilder prevPathElements = new StringBuilder("/");
        if (pathElements != null && pathElements.length > 0) {
            for (final String singleDir : pathElements) {
                if (singleDir.length() > 0) {
                    prevPathElements.append(singleDir).append("/");
                    try {
                        createAndChangeSingleDirectory(singleDir);
                    } catch (final Throwable t) {
                        throw new RuntimeException("At: " + prevPathElements, t);
                    }
                }
            }
        }
    }

    private void changeDirectoryOnly() {
        try {
            final String absDir = getAbsoluteDirectory();
            final String[] pathElements = absDir.split("/");
            if (pathElements != null && pathElements.length > 0) {
                for (final String singleDir : pathElements) {
                    if (singleDir.length() > 0) {
                        finalizer.ftpClient.changeDirectory(singleDir);
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createAndChangeSingleDirectory(final String singleDir) throws Exception {
        try {
            finalizer.ftpClient.changeDirectory(singleDir);
        } catch (final FTPException e) {
            finalizer.ftpClient.createDirectory(singleDir);
            finalizer.ftpClient.changeDirectory(singleDir);
        }
    }

    @Override
    public FtpFileChannel createDirectory() {
        connect(false);
        createAndChangeDirectory();
        directoryCreated = true;
        return this;
    }

    @Override
    public boolean isConnected() {
        return finalizer != null && finalizer.ftpClient != null && finalizer.ftpClient.isConnected()
                && isAuthenticated();
    }

    @Override
    public boolean exists() {
        connect(false);
        if (filename == null) {
            try {
                final String currentDir = finalizer.ftpClient.currentDirectory();
                boolean exists;
                try {
                    finalizer.ftpClient.changeDirectory(getAbsoluteDirectory());
                    exists = true;
                } catch (final Exception e) {
                    exists = false;
                } finally {
                    try {
                        finalizer.ftpClient.changeDirectory(currentDir);
                    } catch (final Exception e) {
                        // ignore
                    }
                }
                return exists;
            } catch (final Exception e) {
                return false;
            }
        }
        return info() != null;
    }

    @Override
    public long length() {
        connect(false);
        try {
            return finalizer.ftpClient.fileSize(getFilename());
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return -1;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FDate lastModified() {
        connect(false);
        try {
            final Date date = finalizer.ftpClient.modifiedDate(getFilename());
            if (date == null) {
                return null;
            } else {
                return new FDate(date);
            }
        } catch (final NumberFormatException | IndexOutOfBoundsException e) {
            return null;
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FtpFileInfo info() {
        connect(false);
        try {
            final FTPFile[] listFiles = finalizer.ftpClient.list(getFilename());
            if (listFiles.length == 0) {
                return null;
            } else if (listFiles.length == 1) {
                return FtpFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory, listFiles[0]);
            } else {
                throw new IllegalStateException("More than one result: " + listFiles.length);
            }
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FtpFileInfo> list() {
        connect(false);
        try {
            return Arrays.asList(FtpFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory,
                    finalizer.ftpClient.list()));
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return Collections.emptyList();
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<FtpFileInfo> listFiles() {
        return (List<FtpFileInfo>) IFileChannel.super.listFiles();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<FtpFileInfo> listDirectories() {
        return (List<FtpFileInfo>) IFileChannel.super.listDirectories();
    }

    private void ensureDirectoryCreated() {
        connect(false);
        if (!directoryCreated) {
            createDirectory();
        }
    }

    @Override
    public FtpFileChannel rename(final String filename) {
        connect(false);
        try {
            finalizer.ftpClient.rename(getFilename(), filename);
            setFilename(filename);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void moveSameType(final IFileChannel targetChannel) {
        connect(false);
        try {
            final FtpFileChannel targetFtp = (FtpFileChannel) targetChannel;
            targetFtp.ensureDirectoryCreated();
            finalizer.ftpClient.rename(getAbsoluteDirectory() + getFilename(),
                    targetFtp.getAbsoluteDirectory() + targetFtp.getFilename());
            setSubDirectory(targetFtp.getSubDirectory());
            setFilename(targetFtp.getFilename());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FtpFileChannel upload(final File file) {
        ensureDirectoryCreated();
        try {
            finalizer.ftpClient.upload(file);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FtpFileChannel upload(final byte[] bytes) {
        return upload(new FastByteArrayInputStream(bytes));
    }

    @Override
    public FtpFileChannel upload(final InputStream input) {
        ensureDirectoryCreated();
        try {
            finalizer.ftpClient.upload(getFilename(), input, 0, 0, null);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.close(input);
        }
    }

    @Override
    public FtpFileChannel download(final File destination) {
        try {
            try (InputStream in = newDownload()) {
                if (in != null) {
                    Files.forceMkdirParent(destination);
                    try (FileOutputStream out = new FileOutputStream(destination)) {
                        IOUtils.copyLarge(in, out);
                    }
                }
            }
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] download() {
        try {
            try (InputStream in = newDownload()) {
                if (in == null) {
                    return null;
                } else {
                    final byte[] bytes = IOUtils.toByteArray(in);
                    return bytes;
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public FtpFileChannel delete() {
        connect(false);
        try {
            finalizer.ftpClient.deleteFile(getFilename());
            return this;
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return this;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
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
        return new ADelegateOutputStream(new TextDescription("%s: uploadOutputStream()", this)) {

            private final File file = downloadLocalTempFile();

            @Override
            protected OutputStream newDelegate() {
                try {
                    return new FileOutputStream(file);
                } catch (final FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                    if (!file.exists()) {
                        Files.write(file, "", Charset.defaultCharset());
                    }
                    finalizer.ftpClient.upload(file);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    file.delete();
                }
            }
        };
    }

    @Override
    public FtpFileChannel reconnect(final boolean createDirectory) {
        close();
        connect(createDirectory);
        return this;
    }

    @Override
    public InputStream newDownload() {
        connect(false);
        final File file = downloadLocalTempFile();
        try {
            finalizer.ftpClient.download(getFilename(), file);
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        if (!file.exists()) {
            return null;
        }
        return new ADelegateInputStream(new TextDescription("%s: downloadInputStream()", this)) {
            @Override
            protected InputStream newDelegate() {
                try {
                    return new DeletingFileInputStream(file);
                } catch (final FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public String toString() {
        return FileChannelInfos.toString(this);
    }

    private static final class FtpFileChannelFinalizer extends AFinalizer {

        private FTPClient ftpClient;

        @Override
        protected void clean() {
            if (ftpClient.isConnected()) {
                try {
                    ftpClient.logout();
                } catch (final Throwable t) {
                    // do nothing
                }
                try {
                    ftpClient.disconnect(true);
                } catch (final Throwable t) {
                    try {
                        ftpClient.disconnect(false);
                    } catch (final Throwable t1) {
                        // do nothing
                    }
                }
            }
            ftpClient = null;
        }

        @Override
        protected boolean isCleaned() {
            return ftpClient == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}