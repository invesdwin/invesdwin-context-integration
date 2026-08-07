package de.invesdwin.context.integration.grid.hadoop.filechannel;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.fs.FileStatus;

import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class HadoopFileInfo implements IFileInfo {

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private final String subDirectory;
    private final FileStatus delegate;

    // Cached attributes retrieved efficiently from Hadoop FileStatus
    private final boolean isDirectory;
    private final boolean isFile;
    private final long length;
    private final FDate lastModified;

    public HadoopFileInfo(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final FileStatus delegate) {
        this.serverUri = serverUri;
        this.baseServerUri = baseServerUri;
        this.baseDirectory = baseDirectory;
        this.subDirectory = subDirectory;
        this.delegate = delegate;

        this.isDirectory = delegate.isDirectory();
        this.isFile = delegate.isFile();
        this.length = delegate.getLen();
        this.lastModified = new FDate(delegate.getModificationTime());
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
    public String getAbsoluteDirectory() {
        return HadoopFileChannel.combinePath(baseDirectory, subDirectory);
    }

    @Override
    public String getFilename() {
        return delegate.getPath().getName();
    }

    @Override
    public boolean isFile() {
        return isFile;
    }

    @Override
    public boolean isDirectory() {
        return isDirectory;
    }

    @Override
    public FDate lastModified() {
        return lastModified;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public FileStatus unwrap() {
        return delegate;
    }

    @Override
    public String toString() {
        return FileChannelInfos.toString(this);
    }

    public static HadoopFileInfo valueOf(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final FileStatus status) {
        if (status == null) {
            return null;
        }
        return new HadoopFileInfo(serverUri, baseServerUri, baseDirectory, subDirectory, status);
    }
}