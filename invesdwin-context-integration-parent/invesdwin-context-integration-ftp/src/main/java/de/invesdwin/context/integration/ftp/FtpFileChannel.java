package de.invesdwin.context.integration.ftp;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
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

    /**
     * https://issues.apache.org/jira/browse/NET-12
     */
    public String getAbsoluteFile() {
        return directory + filename;
    }

    public byte[] getEmptyFileContent() {
        return emptyFileContent;
    }

    public void setEmptyFileContent(final byte[] emptyFileContent) {
        this.emptyFileContent = emptyFileContent;
    }

    public void createUniqueFilename(final String filenamePrefix, final String filenameSuffix) {
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
            ftpClient.connect(serverUri.getHost(), serverUri.getPort());
            final int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                throw new IllegalStateException("FTP server refused connection.");
            }
            if (!ftpClient.login(FtpClientProperties.USERNAME, FtpClientProperties.PASSWORD)) {
                throw new IllegalStateException("Login failed");
            }
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            //be a bit more firewall friendly
            ftpClient.enterLocalPassiveMode();
            changeDirectory();
            if (filename == null) {
                createUniqueFilename(FtpFileChannel.class.getSimpleName() + "_", ".channel");
            }
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    /**
     * http://www.codejava.net/java-se/networking/ftp/creating-nested-directory-structure-on-a-ftp-server
     */
    private void changeDirectory() throws IOException {
        final String[] pathElements = directory.split("/");
        if (pathElements != null && pathElements.length > 0) {
            for (final String singleDir : pathElements) {
                if (singleDir.length() > 0) {
                    final boolean existed = ftpClient.changeWorkingDirectory(singleDir);
                    if (!existed) {
                        final boolean created = ftpClient.makeDirectory(singleDir);
                        if (created) {
                            ftpClient.changeWorkingDirectory(singleDir);
                        }
                    }
                }
            }
        }
    }

    public boolean isConnected() {
        return ftpClient != null && ftpClient.isConnected();
    }

    public boolean exists() {
        final InputStream in = newInputStream();
        if (in == null) {
            return false;
        } else {
            IOUtils.closeQuietly(in);
            return true;
        }
    }

    private void assertConnected() {
        Assertions.checkNotNull(ftpClient, "Please call connect() first");
        Assertions.checkTrue(ftpClient.isConnected(), "Not connected yet");
    }

    public void write(final InputStream in) {
        assertConnected();
        try (InputStream autoClose = in) {
            if (!ftpClient.storeFile(getAbsoluteFile(), autoClose)) {
                throw new IllegalStateException("storeFile returned false: " + getAbsoluteFile());
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream read() {
        assertConnected();
        try {
            return ftpClient.retrieveFileStream(getAbsoluteFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean delete() {
        assertConnected();
        try {
            return ftpClient.deleteFile(getAbsoluteFile());
        } catch (final IOException e) {
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
            return ftpClient.storeFileStream(getAbsoluteFile());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream newInputStream() {
        assertConnected();
        try {
            final InputStream inputStream = ftpClient.retrieveFileStream(getAbsoluteFile());
            final int returnCode = ftpClient.getReplyCode();
            if (inputStream == null || returnCode == FTPReply.FILE_UNAVAILABLE) {
                return null;
            }
            return inputStream;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
