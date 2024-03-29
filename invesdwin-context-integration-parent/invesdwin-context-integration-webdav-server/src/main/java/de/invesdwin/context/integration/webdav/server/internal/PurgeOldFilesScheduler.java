package de.invesdwin.context.integration.webdav.server.internal;

import java.io.File;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.webdav.WebdavClientProperties;
import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.FDate;
import jakarta.inject.Named;

@Named
@NotThreadSafe
public class PurgeOldFilesScheduler implements IStartupHook {

    @SkipParallelExecution
    @Scheduled(cron = "0 0 0 * * ?") //check every day
    public void purgeOldFiles() {
        if (WebdavServerProperties.PURGE_FILES_OLDER_THAN_DURATION == null
                || !WebdavServerProperties.WORKING_DIRECTORY.exists()) {
            return;
        }
        final FDate threshold = new FDate().subtract(WebdavServerProperties.PURGE_FILES_OLDER_THAN_DURATION);
        final Iterator<File> filesToDelete = Files.iterateFiles(WebdavServerProperties.WORKING_DIRECTORY,
                new AgeFileFilter(threshold.dateValue(), true), new NotFileFilter(
                        new NameFileFilter(WebdavClientProperties.PROTECTED_FOLDER_NAME, IOCase.INSENSITIVE)));
        while (filesToDelete.hasNext()) {
            final File fileToDelete = filesToDelete.next();
            fileToDelete.delete();
        }
        for (final File f : WebdavServerProperties.WORKING_DIRECTORY.listFiles()) {
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
