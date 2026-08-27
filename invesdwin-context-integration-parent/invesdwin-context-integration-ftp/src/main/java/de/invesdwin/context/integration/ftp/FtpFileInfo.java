package de.invesdwin.context.integration.ftp;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPaths;
import de.invesdwin.util.time.date.FDate;
import it.sauronsoftware.ftp4j.FTPFile;

@Immutable
public class FtpFileInfo implements IFileInfo {

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private final String subDirectory;
    private final FTPFile delegate;

    public FtpFileInfo(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final FTPFile delegate) {
        this.serverUri = serverUri;
        this.baseServerUri = baseServerUri;
        this.baseDirectory = baseDirectory;
        this.subDirectory = subDirectory;
        this.delegate = delegate;
    }

    @Override
    public URI getServerUri() {
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
    public String getFilename() {
        return delegate.getName();
    }

    @Override
    public boolean isFile() {
        return delegate.getType() == FTPFile.TYPE_FILE;
    }

    @Override
    public boolean isDirectory() {
        return delegate.getType() == FTPFile.TYPE_DIRECTORY;
    }

    @Override
    public FDate lastModified() {
        return FDate.valueOf(delegate.getModifiedDate());
    }

    @Override
    public long length() {
        return delegate.getSize();
    }

    @Override
    public FTPFile unwrap() {
        return delegate;
    }

    @Override
    public String toString() {
        return FileChannelPaths.toString(this);
    }

    public static FtpFileInfo[] valueOf(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final FTPFile[] array) {
        final FtpFileInfo[] result = new FtpFileInfo[array.length];
        for (int i = 0; i < array.length; i++) {
            final FTPFile file = array[i];
            if (file == null) {
                continue;
            }
            result[i] = valueOf(serverUri, baseServerUri, baseDirectory, subDirectory, file);
        }
        return result;
    }

    public static FtpFileInfo valueOf(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final FTPFile file) {
        if (file == null) {
            return null;
        }
        return new FtpFileInfo(serverUri, baseServerUri, baseDirectory, subDirectory, file);
    }

}