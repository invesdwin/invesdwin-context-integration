package de.invesdwin.context.integration.ftp.internal;

import java.io.File;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ftp.FtpClientProperties;
import de.invesdwin.util.time.fdate.FDate;

@Named
@NotThreadSafe
public class PurgeOldTempFilesScheduler implements IStartupHook {

    @SkipParallelExecution
    @Scheduled(cron = "0 0 0 * * ?") //check every day
    public void purgeOldFiles() {
        if (FtpClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION == null) {
            return;
        }
        final FDate threshold = new FDate().subtract(FtpClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION);
        final Iterator<File> filesToDelete = FileUtils.iterateFiles(FtpClientProperties.TEMP_DIRECTORY,
                new AgeFileFilter(threshold.dateValue(), true), TrueFileFilter.INSTANCE);
        while (filesToDelete.hasNext()) {
            final File fileToDelete = filesToDelete.next();
            fileToDelete.delete();
        }
        for (final File f : FtpClientProperties.TEMP_DIRECTORY.listFiles()) {
            maybeDeleteEmptyDirectories(f);
        }
    }

    /**
     * https://stackoverflow.com/questions/26017545/delete-all-empty-folders-in-java
     */
    private long maybeDeleteEmptyDirectories(final File f) {
        final String[] listFiles = f.list();
        long totalSize = 0;
        for (final String file : listFiles) {

            final File folder = new File(f, file);
            if (folder.isDirectory()) {
                totalSize += maybeDeleteEmptyDirectories(folder);
            } else {
                totalSize += folder.length();
            }
        }

        if (totalSize == 0) {
            f.delete();
        }

        return totalSize;
    }

    @Override
    public void startup() throws Exception {
        purgeOldFiles();
    }

}
