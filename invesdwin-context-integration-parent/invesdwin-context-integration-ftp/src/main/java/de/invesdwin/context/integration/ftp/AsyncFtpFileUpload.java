package de.invesdwin.context.integration.ftp;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.retry.ARetryingRunnable;
import de.invesdwin.context.integration.retry.RetryDisabledRuntimeException;
import de.invesdwin.context.integration.retry.RetryLaterRuntimeException;
import de.invesdwin.context.integration.retry.RetryOriginator;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;

@NotThreadSafe
public class AsyncFtpFileUpload implements Runnable {

    public static final String FINISHED_FILENAME_SUFFIX = ".finished";
    public static final int MAX_TRIES = 3;
    private static final int MAX_PARALLEL_UPLOADS = 2;

    private static final WrappedExecutorService EXECUTOR = Executors
            .newFixedThreadPool(AsyncFtpFileUpload.class.getSimpleName(), MAX_PARALLEL_UPLOADS);

    private final FtpFileChannel channel;
    private final String channelFileName;
    private final File temporaryInputFile;
    private int tries = 0;

    public AsyncFtpFileUpload(final FtpFileChannel channel, final File temporaryInputFile) {
        this.channel = channel;
        this.temporaryInputFile = temporaryInputFile;
        this.channelFileName = channel.getFilename();
        Assertions.checkNotNull(channelFileName);
    }

    @Override
    public void run() {
        new ARetryingRunnable(new RetryOriginator(AsyncFtpFileUpload.class, "run", channel, temporaryInputFile)) {
            @Override
            protected void runRetryable() throws Exception {
                try {
                    cleanupForUpload();
                    EXECUTOR.awaitPendingCount(MAX_PARALLEL_UPLOADS);
                    upload();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (final Throwable t) {
                    throw handleRetry(t);
                }
            }

        };
    }

    private void cleanupForUpload() {
        if (!channel.isConnected()) {
            channel.connect();
        }
        channel.setFilename(channelFileName);
        channel.delete();
        channel.setFilename(channelFileName + FINISHED_FILENAME_SUFFIX);
        channel.delete();
    }

    private void upload() {
        EXECUTOR.execute(
                new ARetryingRunnable(new RetryOriginator(AsyncFtpFileUpload.class, "upload", channel, temporaryInputFile)) {
                    @Override
                    protected void runRetryable() throws Exception {
                        try {
                            cleanupForUpload();
                            channel.setFilename(channelFileName);
                            channel.write(temporaryInputFile);
                            deleteInputFileAutomatically();
                            closeChannelAutomatically();
                        } catch (final Throwable t) {
                            throw handleRetry(t);
                        }
                    }
                });
    }

    private RuntimeException handleRetry(final Throwable t) {
        tries++;
        if (tries >= MAX_TRIES) {
            return new RetryDisabledRuntimeException(
                    "Aborting upload retry after " + tries + " tries because: " + t.toString(), t);
        } else {
            return new RetryLaterRuntimeException(t);
        }
    }

    protected void deleteInputFileAutomatically() {
        temporaryInputFile.delete();
    }

    protected void closeChannelAutomatically() {
        channel.close();
    }

}
