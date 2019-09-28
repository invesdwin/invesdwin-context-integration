package de.invesdwin.context.integration.ftp.internal;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Named;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ftp.FtpClientProperties;
import de.invesdwin.util.lang.Files;

@Named
@NotThreadSafe
public class PurgeOldTempFilesScheduler implements IStartupHook {

    @Scheduled(cron = "0 0 0 * * ?") //check every day
    public void purgeOldFiles() {
        if (FtpClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION == null
                || !FtpClientProperties.TEMP_DIRECTORY.exists()) {
            return;
        }
        Files.purgeOldFiles(FtpClientProperties.TEMP_DIRECTORY,
                FtpClientProperties.PURGE_TEMP_FILES_OLDER_THAN_DURATION);
    }

    @Override
    public void startup() throws Exception {
        purgeOldFiles();
    }

}
