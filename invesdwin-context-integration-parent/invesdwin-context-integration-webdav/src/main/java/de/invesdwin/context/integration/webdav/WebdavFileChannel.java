package de.invesdwin.context.integration.webdav;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import com.github.sardine.impl.SardineException;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.path.UriFileChannelPath;
import de.invesdwin.context.integration.filechannel.info.path.FileChannelPaths;
import de.invesdwin.context.integration.filechannel.info.path.IFileChannelPath;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.Objects;
import de.invesdwin.util.lang.UUIDs;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.streams.delegate.ADelegateOutputStream;
import de.invesdwin.util.time.date.FDate;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;

@NotThreadSafe
public class WebdavFileChannel implements IFileChannel {

    public static final String DEFAULT_SERVER_URI_STR = "webdav:///";
    public static final URI DEFAULT_SERVER_URI = URI.create(DEFAULT_SERVER_URI_STR);
    public static final Supplier<URI> DEFAULT_SERVER_URI_F = () -> DEFAULT_SERVER_URI;

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private String subDirectory = "";
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private boolean directoryValidated = false;

    @GuardedBy("this")
    private transient WebdavFileChannelFinalizer finalizer;

    public WebdavFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri));
    }

    public WebdavFileChannel(final URI serverUri) {
        this(UriFileChannelPath.valueOf(serverUri, DEFAULT_SERVER_URI_F));
    }

    public WebdavFileChannel(final IFileChannelPath path) {
        this.serverUri = path.getServerUri();
        this.baseServerUri = path.getBaseServerUri();
        this.baseDirectory = path.getAbsoluteDirectory();
        this.filename = path.getFilename();
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubDirectory(final String subDirectory) {
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.filename = filename;
        instance.setSubDirectory(subDirectory);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withBaseServerUri(final URI baseServerUri) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(baseServerUri, getBaseDirectory());
        //CHECKSTYLE:OFF
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
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
    public WebdavFileChannel withBaseServerUri(final String baseServerUri) {
        //CHECKSTYLE:ON
        return withBaseServerUri(URIs.asUri(baseServerUri));
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withBaseDirectory(final String baseDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(getBaseServerUri(), baseDirectory);
        //CHECKSTYLE:OFF
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
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
    public WebdavFileChannel withAbsoluteDirectory(final String absoluteDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelPaths.newDirectoryUri(getBaseServerUri(), absoluteDirectory);
        //CHECKSTYLE:OFF
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        if (getFilename() != null) {
            instance.setFilename(getFilename());
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubPath(final String subPath) {
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(subPath);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubPath(final Path path) {
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(path);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withFilename(final String filename) {
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        //CHECKSTYLE:ON
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        instance.setFilename(filename);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withAbsolutePath(final String path) {
        //CHECKSTYLE:ON
        if (Strings.isBlank(path)) {
            //CHECKSTYLE:OFF
            final WebdavFileChannel instance = new WebdavFileChannel(getBaseServerUri());
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath((String) null);
            return instance;
        }
        if (path.contains("://")) {
            return (WebdavFileChannel) FileChannelRegistry.newInstance(path);
        } else {
            //CHECKSTYLE:OFF
            final WebdavFileChannel instance = new WebdavFileChannel(getBaseServerUri());
            //CHECKSTYLE:ON
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath(path);
            return instance;
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withAbsolutePath(final Path path) {
        //CHECKSTYLE:ON
        return withAbsolutePath(path != null ? path.toString() : null);
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
    public WebdavFileChannel setSubDirectory(final String subDirectory) {
        final String dirBefore = getAbsoluteDirectory();
        this.subDirectory = subDirectory != null ? subDirectory : "";
        if (!Objects.equals(dirBefore, getAbsoluteDirectory())) {
            directoryValidated = false;
        }
        return this;
    }

    @Override
    public WebdavFileChannel setFilename(final String filename) {
        this.filename = filename;
        return this;
    }

    @Override
    public WebdavFileChannel setSubPath(final String path) {
        IFileChannel.super.setSubPath(path);
        return this;
    }

    @Override
    public WebdavFileChannel setSubPath(final Path path) {
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
    public WebdavFileChannel setEmptyFileContent(final byte[] emptyFileContent) {
        this.emptyFileContent = emptyFileContent;
        return this;
    }

    @Override
    public WebdavFileChannel createUniqueFile() {
        return createUniqueFile(WebdavFileChannel.class.getSimpleName() + "_", ".channel");
    }

    @Override
    public WebdavFileChannel createUniqueFile(final String filenamePrefix, final String filenameSuffix) {
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

    public Sardine getWebdavClient() {
        connect(false);
        return finalizer.webdavClient;
    }

    @Override
    public WebdavFileChannel connect() {
        return connect(true);
    }

    @Override
    public WebdavFileChannel connect(final boolean createDirectory) {
        try {
            if (finalizer == null) {
                finalizer = new WebdavFileChannelFinalizer();
            }
            if (finalizer.webdavClient != null) {
                if (createDirectory && !directoryValidated) {
                    createDirectory();
                }
                return this;
            }
            finalizer.webdavClient = login();
            finalizer.webdavClient.enablePreemptiveAuthentication(URIs.asUrl(serverUri));
            finalizer.register(this);
            if (createDirectory && !directoryValidated) {
                createDirectory();
            }
            return this;
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void ensureDirectoryCreated() {
        connect(false);
        if (!directoryValidated) {
            try {
                if (!finalizer.webdavClient.exists(getDirectoryUri().toString())) {
                    createDirectory();
                } else {
                    directoryValidated = true;
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public WebdavFileChannel createDirectory() {
        connect(false);
        final String[] pathElements = getAbsoluteDirectory().split("/");
        final StringBuilder prevPathElements = new StringBuilder("/");
        if (pathElements != null && pathElements.length > 0) {
            for (final String singleDir : pathElements) {
                if (singleDir.length() > 0) {
                    prevPathElements.append(singleDir).append("/");
                    try {
                        createSingleDirectory(prevPathElements.toString());
                    } catch (final Throwable t) {
                        throw new RuntimeException("At: " + prevPathElements, t);
                    }
                }
            }
        }
        directoryValidated = true;
        return this;
    }

    private void createSingleDirectory(final String singleDir) throws Exception {
        try {
            finalizer.webdavClient.createDirectory(baseServerUri.toString() + singleDir);
        } catch (final SardineException e) {
            if (e.getStatusCode() == HttpStatus.SC_METHOD_NOT_ALLOWED || e.getStatusCode() == HttpStatus.SC_CONFLICT
                    || e.getStatusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                return;
            } else {
                throw e;
            }
        }
    }

    protected Sardine login() {
        final String username;
        final String password;

        if (serverUri != null && serverUri.getUserInfo() != null) {
            final String userInfo = serverUri.getUserInfo();
            final int colonIdx = userInfo.indexOf(':');
            if (colonIdx != -1) {
                username = userInfo.substring(0, colonIdx);
                password = userInfo.substring(colonIdx + 1);
            } else {
                username = userInfo;
                password = ""; // or handle empty password as appropriate
            }
        } else {
            username = WebdavClientProperties.USERNAME;
            password = WebdavClientProperties.PASSWORD;
        }

        return SardineFactory.begin(username, password);
    }

    @Override
    public boolean isConnected() {
        return finalizer != null && finalizer.webdavClient != null;
    }

    @Override
    public boolean exists() {
        connect(false);
        try {
            if (filename == null) {
                return finalizer.webdavClient.exists(getDirectoryUri().toString());
            }
            return finalizer.webdavClient.exists(getFileUri().toString());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long length() {
        final WebdavFileInfo info = info();
        if (info == null) {
            return -1;
        }
        return info.length();
    }

    @Override
    public FDate lastModified() {
        final WebdavFileInfo info = info();
        if (info == null) {
            return null;
        }
        return info.lastModified();
    }

    @Override
    public WebdavFileInfo info() {
        connect(false);
        try {
            final List<DavResource> listFiles = finalizer.webdavClient.list(getFileUri().toString());
            if (listFiles.size() == 0) {
                return null;
            } else if (listFiles.size() == 1) {
                return WebdavFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory, listFiles.get(0));
            } else {
                throw new IllegalStateException("More than one result: " + listFiles.size());
            }
        } catch (final SardineException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<WebdavFileInfo> list() {
        connect(false);
        try {
            final List<DavResource> list = finalizer.webdavClient.list(getDirectoryUri().toString());
            if (!list.isEmpty() && list.get(0).getPath().endsWith(Strings.putSuffix(getAbsoluteDirectory(), "/"))) {
                list.remove(0);
            }
            return WebdavFileInfo.valueOf(serverUri, baseServerUri, baseDirectory, subDirectory, list);
        } catch (final SardineException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WebdavFileInfo> listFiles() {
        return (List<WebdavFileInfo>) IFileChannel.super.listFiles();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<WebdavFileInfo> listDirectories() {
        return (List<WebdavFileInfo>) IFileChannel.super.listDirectories();
    }

    @Override
    public WebdavFileChannel rename(final String filename) {
        connect(false);
        try {
            finalizer.webdavClient.move(getFileUri().toString(),
                    FileChannelPaths.newFileUri(baseServerUri, getAbsoluteDirectory(), filename).toString());
            setFilename(filename);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    @Override
    public void moveSameType(final IFileChannel targetChannel) {
        connect(false);
        try {
            final WebdavFileChannel targetWebdav = (WebdavFileChannel) targetChannel;
            targetWebdav.ensureDirectoryCreated();
            finalizer.webdavClient.move(getFileUri().toString(), targetWebdav.getFileUri().toString());
            setSubDirectory(targetWebdav.getSubDirectory());
            setFilename(targetWebdav.getFilename());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WebdavFileChannel upload(final File file) {
        ensureDirectoryCreated();
        try {
            finalizer.webdavClient.put(getFileUri().toString(), file, null);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WebdavFileChannel upload(final byte[] bytes) {
        return upload(new FastByteArrayInputStream(bytes));
    }

    @Override
    public WebdavFileChannel upload(final InputStream input) {
        ensureDirectoryCreated();
        try {
            finalizer.webdavClient.put(getFileUri().toString(), input);
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.close(input);
        }
    }

    @Override
    public WebdavFileChannel download(final File destination) {
        try (InputStream in = newDownload()) {
            if (in != null) {
                Files.forceMkdirParent(destination);
                try (FileOutputStream out = new FileOutputStream(destination)) {
                    IOUtils.copyLarge(in, out);
                }
            }
            return this;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] downloadBytes() {
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
    public WebdavFileChannel delete() {
        connect(false);
        try {
            finalizer.webdavClient.delete(getFileUri().toString());
            return this;
        } catch (final SardineException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return this;
            } else {
                throw new RuntimeException(e);
            }
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
        directoryValidated = false;
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
                    upload(file);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    file.delete();
                }
            }
        };
    }

    @Override
    public WebdavFileChannel reconnect(final boolean createDirectory) {
        close();
        connect(createDirectory);
        return this;
    }

    @Override
    public InputStream newDownload() {
        connect(false);
        try {
            return finalizer.webdavClient.get(getFileUri().toString());
        } catch (final SardineException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return FileChannelPaths.toString(this);
    }

    private static final class WebdavFileChannelFinalizer extends AFinalizer {
        private Sardine webdavClient;

        @Override
        protected void clean() {
            try {
                webdavClient.shutdown();
            } catch (final IOException e) {
                //ignore
            }
            webdavClient = null;
        }

        @Override
        protected boolean isCleaned() {
            return webdavClient == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }
    }
}