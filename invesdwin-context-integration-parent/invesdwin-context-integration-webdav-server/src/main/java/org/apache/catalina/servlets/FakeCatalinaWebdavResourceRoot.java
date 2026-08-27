package org.apache.catalina.servlets;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.concurrent.Immutable;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.TrackedWebResource;
import org.apache.catalina.WebResource;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.WebResourceSet;
import org.apache.catalina.webresources.EmptyResource;
import org.apache.catalina.webresources.FileResource;

import de.invesdwin.util.lang.Files;

@Immutable
public class FakeCatalinaWebdavResourceRoot implements WebResourceRoot {

    private final FakeCatalinaContext context;
    private final Path rootDir;

    public FakeCatalinaWebdavResourceRoot(final FakeCatalinaContext context, final Path rootDir) {
        this.context = context;
        this.rootDir = rootDir;
        try {
            Files.createDirectories(rootDir);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to initialize root directory: " + rootDir, e);
        }
    }

    private Path resolvePath(final String path) {
        final String normalized = path.startsWith("/") ? path.substring(1) : path;
        final Path resolved = rootDir.resolve(normalized).normalize();

        // Security check: ensure the resolved path stays strictly within rootDir
        if (!resolved.toAbsolutePath().startsWith(rootDir.toAbsolutePath())) {
            throw new SecurityException("Path traversal attempt detected: " + path);
        }
        return resolved;
    }

    @Override
    public WebResource getResource(final String path) {
        final Path file = resolvePath(path);
        if (!Files.exists(file)) {
            return new EmptyResource(this, path);
        }
        // Instantiate Tomcat's built-in FileResource directly
        return new FileResource(this, path, file.toFile(), false, null);
    }

    @Override
    public String[] list(final String path) {
        final Path dir = resolvePath(path);
        if (!Files.isDirectory(dir)) {
            return new String[0];
        }
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.map(p -> p.getFileName().toString()).toArray(String[]::new);
        } catch (final IOException e) {
            return new String[0];
        }
    }

    @Override
    public boolean mkdir(final String path) {
        final Path dir = resolvePath(path);
        try {
            Files.createDirectories(dir);
            return true;
        } catch (final IOException e) {
            return false;
        }
    }

    @Override
    public boolean write(final String path, final InputStream is, final boolean overwrite) {
        final Path file = resolvePath(path);
        try {
            Files.createDirectories(file.getParent());
            if (!overwrite && Files.exists(file)) {
                return false;
            }
            Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (final IOException e) {
            return false;
        }
    }

    public boolean move(final String path, final String destinationPath) {
        try {
            final Path sourcePath = resolvePath(path);
            final Path destPath = resolvePath(destinationPath);

            // Ensure destination parent directories exist
            Files.createDirectories(destPath.getParent());

            // Performs fast native rename if on same volume, or automatic copy-and-delete if cross-volume
            Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (final IOException e) {
            return false;
        }
    }

    @Override
    public WebResource[] listResources(final String path) {
        final Path dir = resolvePath(path);
        if (!Files.isDirectory(dir)) {
            return new WebResource[0];
        }

        // Ensure the base web path ends with a slash so we can append child names
        final String basePath = path.endsWith("/") ? path : path + "/";

        try (Stream<Path> stream = Files.list(dir)) {
            return stream.map(p -> {
                final String childPath = basePath + p.getFileName().toString();
                // Instantiate Tomcat's built-in FileResource directly just like in getResource()
                return new FileResource(this, childPath, p.toFile(), true, null);
            }).toArray(WebResource[]::new);
        } catch (final IOException e) {
            return new WebResource[0];
        }
    }

    // --- Delegated / Supported Defaults ---

    @Override
    public WebResource[] getResources(final String path) {
        final WebResource res = getResource(path);
        return res.exists() ? new WebResource[] { res } : new WebResource[0];
    }

    @Override
    public WebResource getClassLoaderResource(final String path) {
        return getResource(path);
    }

    @Override
    public WebResource[] getClassLoaderResources(final String path) {
        return getResources(path);
    }

    @Override
    public LifecycleState getState() {
        return LifecycleState.STARTED;
    }

    @Override
    public String getStateName() {
        return LifecycleState.STARTED.name();
    }

    // --- Unsupported / Unneeded Operations ---

    @Override
    public void addLifecycleListener(final LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LifecycleListener[] findLifecycleListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLifecycleListener(final LifecycleListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init() throws LifecycleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() throws LifecycleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() throws LifecycleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() throws LifecycleException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listWebAppPaths(final String path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createWebResourceSet(final ResourceSetType type, final String webAppMount, final URL url,
            final String internalPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createWebResourceSet(final ResourceSetType type, final String webAppMount, final String base,
            final String archivePath, final String internalPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPreResources(final WebResourceSet webResourceSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WebResourceSet[] getPreResources() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addJarResources(final WebResourceSet webResourceSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WebResourceSet[] getJarResources() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPostResources(final WebResourceSet webResourceSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WebResourceSet[] getPostResources() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void setContext(final Context context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAllowLinking(final boolean allowLinking) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getAllowLinking() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCachingAllowed(final boolean cachingAllowed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCachingAllowed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCacheTtl(final long ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCacheTtl() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCacheMaxSize(final long cacheMaxSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCacheMaxSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCacheObjectMaxSize(final int cacheObjectMaxSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCacheObjectMaxSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTrackLockedFiles(final boolean trackLockedFiles) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getTrackLockedFiles() {
        return false;
    }

    @Override
    public void setArchiveIndexStrategy(final String archiveIndexStrategy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getArchiveIndexStrategy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ArchiveIndexStrategy getArchiveIndexStrategyEnum() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void backgroundProcess() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerTrackedResource(final TrackedWebResource trackedResource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterTrackedResource(final TrackedWebResource trackedResource) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<URL> getBaseUrls() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void gc() {
        throw new UnsupportedOperationException();
    }

}