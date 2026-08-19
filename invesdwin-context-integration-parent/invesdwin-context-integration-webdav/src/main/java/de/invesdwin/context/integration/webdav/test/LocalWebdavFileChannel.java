package de.invesdwin.context.integration.webdav.test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.time.date.FDate;

@NotThreadSafe
public class LocalWebdavFileChannel implements IFileChannel {

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private final IFileChannel localDelegate;

    public LocalWebdavFileChannel(final URI serverUri) {
        if (serverUri == null) {
            throw new NullPointerException("serverUri should not be null");
        }
        this.serverUri = serverUri;
        this.baseServerUri = FileChannelInfos.extractBaseServerUri(this.serverUri, null);
        this.baseDirectory = FileChannelInfos.extractBaseDirectory(this.serverUri);

        // Build cache directory targeting the directory path rather than the file path
        final URI directoryUri = FileChannelInfos.newDirectoryUri(baseServerUri, getAbsoluteDirectory());
        final File localTargetDir = new File(ContextProperties.getCacheDirectory(),
                LocalWebdavFileChannel.class.getSimpleName() + "/"
                        + Strings.removeStart(Files.normalizePath(directoryUri.toString()), "/"));

        final IFileChannel delegate = FileChannelRegistry.newInstance(localTargetDir.toURI());
        final String filename = FileChannelInfos.extractFileName(serverUri);
        if (filename != null) {
            delegate.setFilename(filename);
        }
        this.localDelegate = delegate;
    }

    public LocalWebdavFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri));
    }

    // --- Fluent Builder Wrappers ---

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withSubDirectory(final String subDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(),
                FileChannelInfos.combinePath(getBaseDirectory(), subDirectory));
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withBaseServerUri(final URI baseServerUri) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(baseServerUri, getBaseDirectory());
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.setSubDirectory(getSubDirectory());
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withBaseServerUri(final String baseServerUri) {
        //CHECKSTYLE:ON
        return withBaseServerUri(URIs.asUri(baseServerUri));
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withBaseDirectory(final String baseDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), baseDirectory);
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.setSubDirectory(getSubDirectory());
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withAbsoluteDirectory(final String absoluteDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), absoluteDirectory);
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withSubPath(final String subPath) {
        //CHECKSTYLE:ON
        if (Strings.isBlank(subPath)) {
            return this;
        }
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(),
                FileChannelInfos.combinePath(getAbsoluteDirectory(), subPath));
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withSubPath(final Path path) {
        //CHECKSTYLE:ON
        return withSubPath(path != null ? path.toString() : null);
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withFilename(final String filename) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newFileUri(getBaseServerUri(), getAbsoluteDirectory(), filename);
        //CHECKSTYLE:OFF
        final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.setFilename(filename);
        instance.setEmptyFileContent(getEmptyFileContent());
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withAbsolutePath(final String path) {
        //CHECKSTYLE:ON
        if (Strings.isBlank(path)) {
            //CHECKSTYLE:OFF
            final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(getBaseServerUri());
            //CHECKSTYLE:ON
            instance.setEmptyFileContent(getEmptyFileContent());
            return instance;
        }
        if (path.contains("://")) {
            //CHECKSTYLE:OFF
            final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(path);
            //CHECKSTYLE:ON
            instance.setEmptyFileContent(getEmptyFileContent());
            return instance;
        } else {
            final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), path);
            //CHECKSTYLE:OFF
            final LocalWebdavFileChannel instance = new LocalWebdavFileChannel(newServerUri);
            //CHECKSTYLE:ON
            instance.setEmptyFileContent(getEmptyFileContent());
            return instance;
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public LocalWebdavFileChannel withAbsolutePath(final Path path) {
        //CHECKSTYLE:ON
        return withAbsolutePath(path != null ? path.toString() : null);
    }

    // --- WebDAV Identity Spoofing ---

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
        return localDelegate.getSubDirectory();
    }

    @Override
    public String getAbsoluteDirectory() {
        return FileChannelInfos.combinePath(baseDirectory, getSubDirectory());
    }

    // --- State Mutations ---

    @Override
    public LocalWebdavFileChannel setSubDirectory(final String subDirectory) {
        localDelegate.setSubDirectory(subDirectory);
        return this;
    }

    @Override
    public LocalWebdavFileChannel setFilename(final String filename) {
        localDelegate.setFilename(filename);
        return this;
    }

    @Override
    public LocalWebdavFileChannel setSubPath(final String path) {
        localDelegate.setSubPath(path);
        return this;
    }

    @Override
    public LocalWebdavFileChannel setSubPath(final Path path) {
        localDelegate.setSubPath(path);
        return this;
    }

    @Override
    public String getFilename() {
        return localDelegate.getFilename();
    }

    @Override
    public byte[] getEmptyFileContent() {
        return localDelegate.getEmptyFileContent();
    }

    @Override
    public LocalWebdavFileChannel setEmptyFileContent(final byte[] emptyFileContent) {
        localDelegate.setEmptyFileContent(emptyFileContent);
        return this;
    }

    @Override
    public LocalWebdavFileChannel createUniqueFile() {
        localDelegate.createUniqueFile();
        return this;
    }

    @Override
    public LocalWebdavFileChannel createUniqueFile(final String filenamePrefix, final String filenameSuffix) {
        localDelegate.createUniqueFile(filenamePrefix, filenameSuffix);
        return this;
    }

    // --- Connection & Lifecycle ---

    @Override
    public LocalWebdavFileChannel connect() {
        localDelegate.connect();
        return this;
    }

    @Override
    public LocalWebdavFileChannel connect(final boolean createDirectory) {
        localDelegate.connect(createDirectory);
        return this;
    }

    @Override
    public LocalWebdavFileChannel createDirectory() {
        localDelegate.createDirectory();
        return this;
    }

    @Override
    public boolean isConnected() {
        return localDelegate.isConnected();
    }

    @Override
    public LocalWebdavFileChannel reconnect(final boolean createDirectory) {
        localDelegate.reconnect(createDirectory);
        return this;
    }

    @Override
    public void close() {
        localDelegate.close();
    }

    // --- File System Operations ---

    @Override
    public boolean exists() {
        return localDelegate.exists();
    }

    @Override
    public long length() {
        return localDelegate.length();
    }

    @Override
    public FDate lastModified() {
        return localDelegate.lastModified();
    }

    @Override
    public IFileInfo info() {
        return localDelegate.info();
    }

    @Override
    public List<? extends IFileInfo> list() {
        return localDelegate.list();
    }

    @Override
    public List<? extends IFileInfo> listFiles() {
        return localDelegate.listFiles();
    }

    @Override
    public List<? extends IFileInfo> listDirectories() {
        return localDelegate.listDirectories();
    }

    @Override
    public LocalWebdavFileChannel rename(final String filename) {
        localDelegate.rename(filename);
        return this;
    }

    @Override
    public void moveSameType(final IFileChannel targetChannel) {
        if (targetChannel instanceof LocalWebdavFileChannel) {
            final LocalWebdavFileChannel targetStub = (LocalWebdavFileChannel) targetChannel;
            localDelegate.moveSameType(targetStub.localDelegate);
            setSubDirectory(targetStub.getSubDirectory());
            setFilename(targetStub.getFilename());
        } else {
            localDelegate.moveSameType(targetChannel);
        }
    }

    // --- I/O Operations ---

    @Override
    public LocalWebdavFileChannel upload(final File file) {
        localDelegate.upload(file);
        return this;
    }

    @Override
    public LocalWebdavFileChannel upload(final byte[] bytes) {
        localDelegate.upload(bytes);
        return this;
    }

    @Override
    public LocalWebdavFileChannel upload(final InputStream input) {
        localDelegate.upload(input);
        return this;
    }

    @Override
    public LocalWebdavFileChannel download(final File destination) {
        localDelegate.download(destination);
        return this;
    }

    @Override
    public byte[] download() {
        return localDelegate.download();
    }

    @Override
    public InputStream newDownload() {
        return localDelegate.newDownload();
    }

    @Override
    public OutputStream newUpload() {
        return localDelegate.newUpload();
    }

    @Override
    public LocalWebdavFileChannel delete() {
        localDelegate.delete();
        return this;
    }

    @Override
    public String toString() {
        return FileChannelInfos.toString(this);
    }
}