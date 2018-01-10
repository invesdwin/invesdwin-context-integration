package de.invesdwin.context.integration.ftp;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import de.invesdwin.norva.marker.ISerializableValueObject;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.math.Bytes;

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
            final String filename = UUIDs.newRandomUUID() + ".channel";
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
            login();
            //be a bit more firewall friendly
            ftpClient.enterLocalPassiveMode();
            createAndChangeDirectory();
            if (filename == null) {
                createUniqueFile();
            }
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void login() throws SocketException, IOException {
        ftpClient.connect(serverUri.getHost(), serverUri.getPort());
        final int reply = ftpClient.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
            throw new IllegalStateException(
                    "FTP server refused connection: " + Arrays.toString(ftpClient.getReplyStrings()));
        }
        if (!ftpClient.login(FtpClientProperties.USERNAME, FtpClientProperties.PASSWORD)) {
            throw new IllegalStateException("Login failed: " + Arrays.toString(ftpClient.getReplyStrings()));
        }
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
    }

    /**
     * http://www.codejava.net/java-se/networking/ftp/creating-nested-directory-structure-on-a-ftp-server
     */
    private void createAndChangeDirectory() throws IOException {
        final String[] pathElements = directory.split("/");
        final StringBuilder prevPathElements = new StringBuilder("/");
        if (pathElements != null && pathElements.length > 0) {
            for (final String singleDir : pathElements) {
                if (singleDir.length() > 0) {
                    prevPathElements.append(singleDir).append("/");
                    createAndChangeSingleDirectory(prevPathElements, singleDir);
                }
            }
        }
    }

    private void createAndChangeSingleDirectory(final StringBuilder prevPathElements, final String singleDir)
            throws IOException {
        final boolean existed = ftpClient.changeWorkingDirectory(singleDir);
        if (!existed) {
            final boolean created = ftpClient.makeDirectory(singleDir);
            if (created) {
                if (!ftpClient.changeWorkingDirectory(singleDir)) {
                    throw new IllegalStateException("Unable to create directory [" + prevPathElements + "]: "
                            + Arrays.toString(ftpClient.getReplyStrings()));
                }
            }
        }
    }

    public boolean isConnected() {
        return ftpClient != null && ftpClient.isConnected();
    }

    public boolean exists() {
        return info() != null;
    }

    public long size() {
        final FTPFile info = info();
        if (info == null) {
            return -1;
        } else {
            return info.getSize();
        }
    }

    public FTPFile info() {
        try {
            final FTPFile[] listFiles = ftpClient.listFiles(getFilename());
            if (listFiles.length == 0) {
                return null;
            } else if (listFiles.length == 1) {
                return listFiles[0];
            } else {
                throw new IllegalStateException("More than one result: " + listFiles.length);
            }
        } catch (final IOException e) {
            throw new RuntimeException(Arrays.toString(ftpClient.getReplyStrings()), e);
        }
    }

    private void assertConnected() {
        Assertions.checkNotNull(ftpClient, "Please call connect() first");
        Assertions.checkTrue(ftpClient.isConnected(), "Not connected yet");
    }

    public void write(final byte[] bytes) {
        write(new ByteArrayInputStream(bytes));
    }

    public void write(final InputStream in) {
        assertConnected();
        try (InputStream autoClose = in) {
            if (!ftpClient.storeFile(getFilename(), autoClose)) {
                throw new IllegalStateException(
                        "storeFile returned false: " + Arrays.toString(ftpClient.getReplyStrings()));
            }
        } catch (final IOException e) {
            throw new RuntimeException(Arrays.toString(ftpClient.getReplyStrings()), e);
        }
    }

    public byte[] read() {
        try (InputStream in = newInputStream()) {
            if (in == null) {
                return null;
            } else {
                return IOUtils.toByteArray(in);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete() {
        assertConnected();
        try {
            if (!ftpClient.deleteFile(getFilename())) {
                throw new IllegalStateException(
                        "deleteFile returned false: " + Arrays.toString(ftpClient.getReplyStrings()));
            }
            if (!ftpClient.completePendingCommand()) {
                throw new IllegalStateException(
                        "completePendingCommand returned false: " + Arrays.toString(ftpClient.getReplyStrings()));
            }
        } catch (final IOException e) {
            throw new RuntimeException(Arrays.toString(ftpClient.getReplyStrings()), e);
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
                    ftpClient.disconnect();
                } catch (final Throwable t) {
                    // do nothing
                }
            }
            ftpClient = null;
        }
    }

    public OutputStream newOutputStream() {
        assertConnected();
        try {
            return ftpClient.storeFileStream(getFilename());
        } catch (final IOException e) {
            throw new RuntimeException(Arrays.toString(ftpClient.getReplyStrings()), e);
        }
    }

    public InputStream newInputStream() {
        assertConnected();
        try {
            final InputStream inputStream = ftpClient.retrieveFileStream(getFilename());
            final int returnCode = ftpClient.getReplyCode();
            if (inputStream == null || returnCode == FTPReply.FILE_UNAVAILABLE) {
                if (inputStream != null) {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                }
                return null;
            }
            return inputStream;
        } catch (final IOException e) {
            throw new RuntimeException(Arrays.toString(ftpClient.getReplyStrings()), e);
        }
    }

}
