package de.invesdwin.context.integration.ftp.server.internal;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.aspects.annotation.SkipParallelExecution;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ftp.server.FtpServerProperties;
import de.invesdwin.util.lang.Files;

@Named
@NotThreadSafe
public class PurgeOldFilesScheduler implements IStartupHook {

    @SkipParallelExecution
    @Scheduled(cron = "0 0 0 * * ?") //check every day
    public void purgeOldFiles() {
        if (FtpServerProperties.PURGE_FILES_OLDER_THAN_DURATION == null
                || !FtpServerProperties.WORKING_DIRECTORY.exists()) {
            return;
        }
        Files.purgeOldFiles(FtpServerProperties.WORKING_DIRECTORY, FtpServerProperties.PURGE_FILES_OLDER_THAN_DURATION);
    }

    @Override
    public void startup() throws Exception {
        purgeOldFiles();
    }

}
