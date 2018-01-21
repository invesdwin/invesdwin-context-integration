package de.invesdwin.context.integration.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.ARetryingCallable;
import de.invesdwin.context.integration.retry.RetryDisabledRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryOriginator;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.streams.ADelegateInputStream;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FTimeUnit;

@NotThreadSafe
public class AsyncFtpFileDownload implements Callable<InputStream> {

    private final FtpFileChannel channel;
    private final String channelFileName;
    private final Duration downloadTimeout;
    private Instant timeoutStart;
    private FDate lastFileModified;
    private Long lastFileSize;
    private int tries = 0;

    public AsyncFtpFileDownload(final FtpFileChannel channel, final Duration downloadTimeout) {
        this.channel = channel;
        this.channelFileName = channel.getFilename();
        Assertions.checkNotNull(channelFileName);
        this.downloadTimeout = downloadTimeout;
    }

    @Override
    public InputStream call() throws Exception {
        final Callable<InputStream> retry = new ARetryingCallable<InputStream>(
                new RetryOriginator(AsyncFtpFileDownload.class, "call", channel)) {
            @Override
            protected InputStream callRetryable() {
                try {
                    if (!channel.isConnected()) {
                        channel.connect();
                    }
                    timeoutStart = new Instant();
                    while (shouldWaitForFinishedFile()) {
                        FTimeUnit.SECONDS.sleep(1);
                        if (timeoutStart.toDuration().isGreaterThan(downloadTimeout)) {
                            throw new TimeoutException(
                                    "Timeout of " + downloadTimeout + " exceeded while downloading: " + channel);
                        }
                    }
                    channel.setFilename(channelFileName);
                    final InputStream input = download();
                    return new ADelegateInputStream() {

                        @Override
                        protected InputStream newDelegate() {
                            return input;
                        }

                        @Override
                        public void close() throws IOException {
                            super.close();
                            deleteChannelFileAutomatically();
                            closeChannelAutomatically();
                        }

                    };
                } catch (InterruptedException | TimeoutException e) {
                    throw new RetryDisabledRuntimeException(e);
                } catch (final Throwable t) {
                    throw handleRetry(t);
                }
            }

        };
        return retry.call();
    }

    protected InputStream download() {
        return channel.downloadInputStream();
    }

    private RuntimeException handleRetry(final Throwable t) {
        tries++;
        if (tries >= AsyncFtpFileUpload.MAX_TRIES) {
            return new RetryDisabledRuntimeException(
                    "Aborting upload retry after " + tries + " tries because: " + t.toString(), t);
        } else {
            return new RetryLaterRuntimeException(t);
        }
    }

    protected void deleteChannelFileAutomatically() {
        channel.setFilename(channelFileName + AsyncFtpFileUpload.FINISHED_FILENAME_SUFFIX);
        channel.delete();
        channel.setFilename(channelFileName);
        channel.delete();
    }

    protected void closeChannelAutomatically() {
        channel.close();
    }

    private boolean shouldWaitForFinishedFile() {
        channel.setFilename(channelFileName + AsyncFtpFileUpload.FINISHED_FILENAME_SUFFIX);
        if (channel.exists()) {
            return false;
        }

        channel.setFilename(channelFileName);
        if (channel.exists()) {
            final FDate fileModified = channel.modified();
            if (lastFileModified == null || lastFileModified.isBefore(fileModified)) {
                lastFileModified = fileModified;
                //reset timeout since upload is still in progress
                timeoutStart = new Instant();
                return true;
            }
            final long fileSize = channel.size();
            if (lastFileSize == null || lastFileSize != fileSize) {
                lastFileSize = fileSize;
                //reset timeout since upload is still in progress
                timeoutStart = new Instant();
                return true;
            }
        }

        return true;
    }

}
