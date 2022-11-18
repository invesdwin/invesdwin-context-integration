package de.invesdwin.context.integration.webdav.internal;

import javax.annotation.concurrent.NotThreadSafe;
import jakarta.inject.Named;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.webdav.WebdavClientProperties;
import de.invesdwin.util.lang.Files;

@Named
@NotThreadSafe
public class PurgeOldTempFilesScheduler implements IStartupHook {

    @SkipParallelExecution
    @Scheduled(cron = "0 0 0 * * ?") //check every day
    public void purgeOldFiles() {
        if (WebdavClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION == null
                || !WebdavClientProperties.TEMP_DIRECTORY.exists()) {
            return;
        }
        Files.purgeOldFiles(WebdavClientProperties.TEMP_DIRECTORY,
                WebdavClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION);
    }

    @Override
    public void startup() throws Exception {
        purgeOldFiles();
    }

}
