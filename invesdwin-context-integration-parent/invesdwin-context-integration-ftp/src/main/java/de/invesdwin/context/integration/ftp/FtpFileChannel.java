package de.invesdwin.context.integration.ftp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.ADelegateInputStream;
import de.invesdwin.util.streams.ADelegateOutputStream;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;
import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPCodes;
import it.sauronsoftware.ftp4j.FTPException;
import it.sauronsoftware.ftp4j.FTPFile;

@NotThreadSafe
public class FtpFileChannel implements Closeable, ISerializableValueObject {

    private final URI serverUri;
    private final String directory;
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private transient FTPClient ftpClient;

    public FtpFileChannel(final URI serverUri, final String directory) {
        this.serverUri = serverUri;
        this.directory = Strings.eventuallyAddSuffix(
                Strings.eventuallyAddPrefix(directory.replace("\\", "/").replaceAll("[/]+", "/"), "/"), "/");
    }

    public URI getServerUri() {
        return serverUri;
    }

    public String getDirectory() {
        return directory;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        if (filename == null) {
            throw new NullPointerException("please call setFilename(...) first");
        }
        return filename;
    }

    public byte[] getEmptyFileContent() {
        return emptyFileContent;
    }

    public void setEmptyFileContent(final byte[] emptyFileContent) {
        this.emptyFileContent = emptyFileContent;
    }

    public void createUniqueFile() {
        createUniqueFile(FtpFileChannel.class.getSimpleName() + "_", ".channel");
    }

    public void createUniqueFile(final String filenamePrefix, final String filenameSuffix) {
        assertConnected();
        while (true) {
            final String filename = filenamePrefix + UUIDs.newPseudorandomUUID() + filenameSuffix;
            setFilename(filename);
            if (!exists()) {
                write(new ByteArrayInputStream(getEmptyFileContent()));
                Assertions.checkTrue(exists());
                break;
            }
        }
    }

    public FTPClient getFtpClient() {
        assertConnected();
        return ftpClient;
    }

    public void connect() {
        try {
            if (ftpClient != null && !ftpClient.isConnected()) {
                close();
            }
            Assertions.checkNull(ftpClient, "Already connected");
            ftpClient = new FTPClient();
            //be a bit more firewall friendly
            ftpClient.setPassive(true);
            final int timeoutSeconds = ContextProperties.DEFAULT_NETWORK_TIMEOUT.intValue(FTimeUnit.SECONDS);
            ftpClient.getConnector().setConnectionTimeout(timeoutSeconds);
            ftpClient.getConnector().setReadTimeout(timeoutSeconds);
            ftpClient.getConnector().setCloseTimeout(timeoutSeconds);
            ftpClient.setType(FTPClient.TYPE_BINARY);
            ftpClient.connect(serverUri.getHost(), serverUri.getPort());
            ftpClient.login(FtpClientProperties.USERNAME, FtpClientProperties.PASSWORD);
            createAndChangeDirectory();
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    /**
     * http://www.codejava.net/java-se/networking/ftp/creating-nested-directory-structure-on-a-ftp-server
     */
    private void createAndChangeDirectory() {
        final String[] pathElements = directory.split("/");
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

    private void createAndChangeSingleDirectory(final String singleDir) throws Exception {
        try {
            ftpClient.changeDirectory(singleDir);
        } catch (final FTPException e) {
            ftpClient.createDirectory(singleDir);
            ftpClient.changeDirectory(singleDir);
        }
    }

    public boolean isConnected() {
        return ftpClient != null && ftpClient.isConnected();
    }

    public boolean exists() {
        return info() != null;
    }

    public long size() {
        try {
            return ftpClient.fileSize(getFilename());
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

    public FDate modified() {
        try {
            final Date date = ftpClient.modifiedDate(getFilename());
            if (date == null) {
                return null;
            } else {
                return new FDate(date);
            }
        } catch (final NumberFormatException | ArrayIndexOutOfBoundsException e) {
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

    public FTPFile info() {
        try {
            final FTPFile[] listFiles = ftpClient.list(getFilename());
            if (listFiles.length == 0) {
                return null;
            } else if (listFiles.length == 1) {
                return listFiles[0];
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

    public List<FTPFile> list() {
        try {
            return Arrays.asList(ftpClient.list());
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

    public List<FTPFile> listFiles() {
        final List<FTPFile> list = list();
        final List<FTPFile> files = new ArrayList<>();
        for (final FTPFile file : list) {
            if (file.getType() == FTPFile.TYPE_FILE) {
                files.add(file);
            }
        }
        return files;
    }

    public List<FTPFile> listDirectories() {
        final List<FTPFile> list = list();
        final List<FTPFile> directories = new ArrayList<>();
        for (final FTPFile directory : list) {
            if (directory.getType() == FTPFile.TYPE_DIRECTORY) {
                directories.add(directory);
            }
        }
        return directories;
    }

    private void assertConnected() {
        Assertions.checkNotNull(ftpClient, "Please call connect() first");
        Assertions.checkTrue(ftpClient.isConnected(), "Not connected yet");
    }

    public void write(final File file) {
        try {
            ftpClient.upload(file);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void write(final byte[] bytes) {
        write(new ByteArrayInputStream(bytes));
    }

    public void write(final InputStream input) {
        assertConnected();
        try {
            ftpClient.upload(getFilename(), input, 0, 0, null);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] read() {
        try {
            try (InputStream in = newInputStream()) {
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

    public void delete() {
        assertConnected();
        try {
            ftpClient.deleteFile(getFilename());
        } catch (final FTPException e) {
            if (e.getCode() == FTPCodes.FILE_ACTION_NOT_TAKEN || e.getCode() == FTPCodes.FILE_NOT_FOUND) {
                return;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    @Override
    public void close() {
        if (ftpClient != null) {
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
    }

    public OutputStream newOutputStream() {
        assertConnected();
        return new ADelegateOutputStream() {

            private final File file = getLocalTempFile();

            @Override
            protected OutputStream newDelegate() {
                try {
                    return new BufferedOutputStream(new FileOutputStream(file));
                } catch (final FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                    if (!file.exists()) {
                        //write an empty file
                        FileUtils.write(file, "", Charset.defaultCharset());
                    }
                    ftpClient.upload(file);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    file.delete();
                }
            }
        };
    }

    public File getLocalTempFile() {
        final File directory = new File(FtpClientProperties.TEMP_DIRECTORY, getDirectory());
        try {
            FileUtils.forceMkdir(directory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final File file = new File(directory, getFilename());
        FileUtils.deleteQuietly(file);
        return file;
    }

    public void reconnect() {
        assertConnected();
        close();
        connect();
    }

    public InputStream newInputStream() {
        assertConnected();
        final File file = getLocalTempFile();
        try {
            ftpClient.download(getFilename(), file);
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
        return new ADelegateInputStream() {

            @Override
            protected InputStream newDelegate() {
                try {
                    return new BufferedInputStream(new FileInputStream(file));
                } catch (final FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() throws IOException {
                super.close();
                file.delete();
            }
        };
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("serverUri", serverUri)
                .add("directory", directory)
                .add("filename", filename)
                .toString();
    }

}
