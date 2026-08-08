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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import com.github.sardine.impl.SardineException;

import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.FileChannelInfos;
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

    private final URI serverUri;
    private final URI baseServerUri;
    private final String baseDirectory;
    private String subDirectory = "";
    private String filename;
    private byte[] emptyFileContent = Bytes.EMPTY_ARRAY;
    private boolean directoryValidated = false;

    @GuardedBy("this")
    private transient WebdavFileChannelFinalizer finalizer;

    public WebdavFileChannel(final URI serverUri) {
        if (serverUri == null) {
            throw new NullPointerException("serverUri should not be null");
        }
        this.serverUri = serverUri;
        this.baseServerUri = FileChannelInfos.extractBaseServerUri(this.serverUri, null);
        this.baseDirectory = FileChannelInfos.extractBaseDirectory(this.serverUri);
    }

    public WebdavFileChannel(final String serverUri) {
        this(serverUri == null ? null : URIs.asUri(serverUri));
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubDirectory(final String subDirectory) {
        //CHECKSTYLE:ON
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        instance.emptyFileContent = emptyFileContent;
        instance.filename = filename;
        instance.setSubDirectory(subDirectory);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withBaseServerUri(final URI baseServerUri) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(baseServerUri, getBaseDirectory());
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        try {
            instance.setFilename(getFilename());
        } catch (final Exception e) {
            // filename not set
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
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), baseDirectory);
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
        instance.emptyFileContent = emptyFileContent;
        instance.setSubDirectory(getSubDirectory());
        try {
            instance.setFilename(getFilename());
        } catch (final Exception e) {
            // filename not set
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withAbsoluteDirectory(final String absoluteDirectory) {
        //CHECKSTYLE:ON
        final URI newServerUri = FileChannelInfos.newDirectoryUri(getBaseServerUri(), absoluteDirectory);
        final WebdavFileChannel instance = new WebdavFileChannel(newServerUri);
        instance.emptyFileContent = emptyFileContent;
        try {
            instance.setFilename(getFilename());
        } catch (final Exception e) {
            // filename not set
        }
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubPath(final String subPath) {
        //CHECKSTYLE:ON
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(subPath);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withSubPath(final Path path) {
        //CHECKSTYLE:ON
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
        instance.emptyFileContent = emptyFileContent;
        instance.setSubPath(path);
        return instance;
    }

    //CHECKSTYLE:OFF
    @Override
    public WebdavFileChannel withFilename(final String filename) {
        //CHECKSTYLE:ON
        final WebdavFileChannel instance = new WebdavFileChannel(serverUri);
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
            final WebdavFileChannel instance = new WebdavFileChannel(getBaseServerUri());
            instance.emptyFileContent = emptyFileContent;
            instance.setSubPath((String) null);
            return instance;
        }
        if (path.contains("://")) {
            return (WebdavFileChannel) FileChannelRegistry.newInstance(path);
        } else {
            final WebdavFileChannel instance = new WebdavFileChannel(getBaseServerUri());
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
    public String getAbsoluteDirectory() {
        return FileChannelInfos.combinePath(baseDirectory, subDirectory);
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
        assertConnected();
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
            if (finalizer.webdavClient != null && !isConnected()) {
                close();
            }
            Assertions.checkNull(finalizer.webdavClient, "Already connected");
            finalizer.webdavClient = login();
            finalizer.webdavClient.enablePreemptiveAuthentication(URIs.asUrl(serverUri));
            finalizer.register(this);
            if (createDirectory) {
                createDirectory();
            }
            return this;
        } catch (final Throwable e) {
            close();
            throw new RuntimeException(e);
        }
    }

    private void ensureDirectoryCreated() {
        assertConnected();
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

    /**
     * http://www.codejava.net/java-se/networking/ftp/creating-nested-directory-structure-on-a-ftp-server
     */
    @Override
    public WebdavFileChannel createDirectory() {
        assertConnected();
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
            //500 might happen when creating directories in parallel, the others when folders already exist or parent folders are missing
            if (e.getStatusCode() == HttpStatus.SC_METHOD_NOT_ALLOWED || e.getStatusCode() == HttpStatus.SC_CONFLICT
                    || e.getStatusCode() == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                return;
            } else {
                throw e;
            }
        }
    }

    protected Sardine login() {
        return SardineFactory.begin(WebdavClientProperties.USERNAME, WebdavClientProperties.PASSWORD);
    }

    @Override
    public boolean isConnected() {
        return finalizer != null && finalizer.webdavClient != null;
    }

    @Override
    public boolean exists() {
        assertConnected();
        try {
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
        assertConnected();
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
        assertConnected();
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

    private void assertConnected() {
        if (!isConnected()) {
            connect(false);
        }
    }

    @Override
    public WebdavFileChannel rename(final String filename) {
        assertConnected();
        try {
            finalizer.webdavClient.move(getFileUri().toString(),
                    FileChannelInfos.newFileUri(baseServerUri, getAbsoluteDirectory(), filename).toString());
            setFilename(filename);
            return this;
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
                    IOUtils.copy(in, out);
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
    public WebdavFileChannel delete() {
        assertConnected();
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

            private final File file = getLocalTempFile();

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
                        //write an empty file
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
    public File getLocalTempFile() {
        final File directory = new File(WebdavClientProperties.TEMP_DIRECTORY, getAbsoluteDirectory());
        try {
            Files.forceMkdir(directory);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final File file = new File(directory, getFilename());
        Files.deleteQuietly(file);
        return file;
    }

    @Override
    public WebdavFileChannel reconnect() {
        close();
        connect();
        return this;
    }

    @Override
    public InputStream newDownload() {
        assertConnected();
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
        return FileChannelInfos.toString(this);
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