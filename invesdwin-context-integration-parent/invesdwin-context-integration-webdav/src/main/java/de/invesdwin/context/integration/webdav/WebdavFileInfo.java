package de.invesdwin.context.integration.webdav;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.github.sardine.DavResource;

import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.util.time.date.FDate;

@Immutable
public class WebdavFileInfo implements IFileInfo {

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private final String subDirectory;
    private final DavResource delegate;

    public WebdavFileInfo(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final DavResource delegate) {
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
    public String getAbsoluteDirectory() {
        return FileChannelInfos.combinePath(baseDirectory, subDirectory);
    }

    @Override
    public String getFilename() {
        return delegate.getName();
    }

    @Override
    public FDate lastModified() {
        return FDate.valueOf(delegate.getModified());
    }

    @Override
    public long length() {
        final Long length = delegate.getContentLength();
        if (length == null) {
            return -1;
        }
        return length.longValue();
    }

    @Override
    public boolean isFile() {
        return !delegate.isDirectory();
    }

    @Override
    public boolean isDirectory() {
        return delegate.isDirectory();
    }

    @Override
    public DavResource unwrap() {
        return delegate;
    }

    @Override
    public String toString() {
        return FileChannelInfos.toString(this);
    }

    public static List<WebdavFileInfo> valueOf(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final List<DavResource> list) {
        final List<WebdavFileInfo> result = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            final DavResource file = list.get(i);
            if (file == null) {
                continue;
            }
            result.add(valueOf(serverUri, baseServerUri, baseDirectory, subDirectory, file));
        }
        return result;
    }

    public static WebdavFileInfo valueOf(final URI serverUri, final URI baseServerUri, final String baseDirectory,
            final String subDirectory, final DavResource file) {
        if (file == null) {
            return null;
        }
        return new WebdavFileInfo(serverUri, baseServerUri, baseDirectory, subDirectory, file);
    }

}