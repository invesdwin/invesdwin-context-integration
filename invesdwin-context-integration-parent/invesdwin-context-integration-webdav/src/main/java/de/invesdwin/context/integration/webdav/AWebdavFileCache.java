package de.invesdwin.context.integration.webdav;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.filechannel.IFileChannel;
import de.invesdwin.context.integration.filechannel.info.IFileInfo;
import de.invesdwin.context.integration.filechannel.registry.FileChannelRegistry;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.concurrent.lock.file.FileChannelLock;
import de.invesdwin.util.concurrent.pool.timeout.ATimeoutObjectPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.description.TextDescription;
import de.invesdwin.util.log.ILogLevel;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;

@ThreadSafe
public abstract class AWebdavFileCache {

    protected final Log log = new Log(this);

    protected final AtomicInteger activeRequests = new AtomicInteger();

    private final ATimeoutObjectPool<IFileChannel> channelPool = new ATimeoutObjectPool<IFileChannel>(
            Duration.TEN_MINUTES, Duration.ONE_MINUTE) {
        @Override
        protected IFileChannel newObject() {
            final WebdavServerDestinationProvider webdavServerDestinationProvider = MergedContext.getInstance()
                    .getBean(WebdavServerDestinationProvider.class);
            return FileChannelRegistry.newDirectory(webdavServerDestinationProvider.getDestination());
        }

        @Override
        protected boolean passivateObject(final IFileChannel element) {
            element.setSubDirectory(null);
            element.setFileName(null);
            return true;
        }

        @Override
        public void invalidateObject(final IFileChannel element) {
            element.close();
        }
    };

    protected abstract boolean isEnabled();

    protected boolean doDownload(final String remoteDirectory, final String remoteFileName, final File localFile,
            final ILogLevel successLogLevel, final String logContextFormat, final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return false;
        }
        final Instant start = new Instant();
        IFileChannel channel = null;
        final int activeRequestsNow = activeRequests.incrementAndGet();
        final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            channel.setFileName(remoteFileName);
            if (!channel.isConnected()) {
                channel.connect();
            }
            if (channel.exists()) {
                final File partFile = new File(localFile.getAbsolutePath() + ".part");
                final File lockFile = new File(partFile.getAbsolutePath() + ".lock");
                final boolean fileExisted = localFile.exists();
                try (FileChannelLock fileLock = new FileChannelLock(lockFile) {
                    @Override
                    protected boolean isThreadLockEnabled() {
                        return true;
                    }
                }) {
                    fileLock.lock();
                    if (!fileExisted && localFile.exists()) {
                        return true;
                    }
                    channel.download(partFile);
                    Files.moveFileQuietly(partFile, localFile);
                    successLogLevel.log(log, "Download from WebDav %s (%s|%s) finished after %s", logContext,
                            activeRequestsNow, activeRequests, start);
                }
                return true;
            } else {
                return false;
            }
        } catch (final Throwable t) {
            log.warn("Download from WebDav %s aborted after %s: %s", logContext, start, Throwables.concatMessages(t));
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
            activeRequests.decrementAndGet();
        }
    }

    protected boolean doUpload(final String remoteDirectory, final String remoteFileName, final File localFile,
            final ILogLevel successLogLevel, final String logContextFormat, final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return true;
        }
        final Instant start = new Instant();
        IFileChannel channel = null;
        final int activeRequestsNow = activeRequests.incrementAndGet();
        final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            channel.setFileName(remoteFileName + ".part");
            if (!channel.isConnected()) {
                channel.connect();
            }
            channel.upload(localFile);
            channel.rename(remoteFileName);
            successLogLevel.log(log, "Upload to WebDav %s (%s|%s) finished after %s", logContext, activeRequestsNow,
                    activeRequests, start);
            return true;
        } catch (final Throwable t) {
            log.warn("Upload to WebDav %s aborted after %s: %s", logContext, start, Throwables.concatMessages(t));
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
            activeRequests.decrementAndGet();
        }
    }

    protected boolean doExists(final String remoteDirectory, final String remoteFileName, final String logContextFormat,
            final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return false;
        }
        IFileChannel channel = null;
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            channel.setFileName(remoteFileName);
            if (!channel.isConnected()) {
                channel.connect();
            }
            return channel.exists();
        } catch (final Throwable t) {
            final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
            log.warn("Exists in WebDav %s aborted: %s", logContext, Throwables.concatMessages(t));
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }

    protected List<? extends IFileInfo> doListFiles(final String remoteDirectory, final String logContextFormat,
            final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return Collections.emptyList();
        }
        IFileChannel channel = null;
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            if (!channel.isConnected()) {
                channel.connect();
            }
            final List<? extends IFileInfo> files = channel.listFiles();
            return files == null ? Collections.emptyList() : files;
        } catch (final Throwable t) {
            final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
            log.warn("ListFiles from WebDav %s aborted: %s", logContext, Throwables.concatMessages(t));
            return Collections.emptyList();
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }

    protected List<? extends IFileInfo> doListDirectories(final String remoteDirectory, final String logContextFormat,
            final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return Collections.emptyList();
        }
        IFileChannel channel = null;
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            if (!channel.isConnected()) {
                channel.connect();
            }
            final List<? extends IFileInfo> directories = channel.listDirectories();
            return directories == null ? Collections.emptyList() : directories;
        } catch (final Throwable t) {
            final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
            log.warn("ListDirectories from WebDav %s aborted: %s", logContext, Throwables.concatMessages(t));
            return Collections.emptyList();
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }

    protected void doDeleteFiles(final String remoteDirectory, final String logContextFormat,
            final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return;
        }
        IFileChannel channel = null;
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            if (!channel.isConnected()) {
                channel.connect();
            }
            final List<? extends IFileInfo> files = channel.listFiles();
            if (files == null || files.isEmpty()) {
                return;
            }
            for (int i = 0; i < files.size(); i++) {
                final IFileInfo file = files.get(i);
                channel.setFileName(file.getFileName());
                channel.delete();
            }
        } catch (final Throwable t) {
            final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
            throw new RetryLaterRuntimeException(TextDescription.format("DeleteFiles from WebDav %s aborted: %s",
                    logContext, Throwables.concatMessages(t)), t);
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }

    protected boolean doDeleteFile(final String remoteDirectory, final String remoteFileName,
            final String logContextFormat, final Object... logContextFormatArgs) {
        if (!isEnabled()) {
            return false;
        }
        IFileChannel channel = null;
        try {
            channel = channelPool.borrowObject();
            channel.setSubDirectory(remoteDirectory);
            channel.setFileName(remoteFileName);
            if (!channel.isConnected()) {
                channel.connect();
            }
            channel.delete();
            return true;
        } catch (final Throwable t) {
            final TextDescription logContext = new TextDescription(logContextFormat, logContextFormatArgs);
            log.warn("Delete from WebDav %s aborted: %s", logContext, Throwables.concatMessages(t));
            return false;
        } finally {
            if (channel != null) {
                channelPool.returnObject(channel);
            }
        }
    }
}