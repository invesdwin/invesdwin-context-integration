package de.invesdwin.context.integration.webdav.server.internal;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.webdav.server.WebdavServerProperties;
import de.invesdwin.util.lang.Files;
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
        Files.purgeOldFiles(WebdavServerProperties.WORKING_DIRECTORY,
                WebdavServerProperties.PURGE_FILES_OLDER_THAN_DURATION);
    }

    @Override
    public void startup() throws Exception {
        purgeOldFiles();
    }

}
