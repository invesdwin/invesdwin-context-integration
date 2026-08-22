package de.invesdwin.context.integration.grid.ignite2.test.bootstrapped.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;
import org.springframework.core.io.Resource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.jar.fork.ForkProcessHelper;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.streams.resource.Resources;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class BootstrappedIgnite2Task implements IgniteCallable<BootstrappedIgnite2Task.TaskResult> {

    public static final String JOB_STATE_CACHE = "igniteJobStateCache";
    public static final String KEY_SERVER_ADDRESS = "serverAddress";

    private final int rank;
    private final int size;
    private final String jobJar;

    public BootstrappedIgnite2Task(final int rank, final int size, final String jobJar) {
        this.rank = rank;
        this.size = size;
        this.jobJar = jobJar;
    }

    @Override
    public TaskResult call() throws Exception {
        final Ignite ignite = Ignition.ignite();
        final IgniteCache<String, String> cache = ignite.getOrCreateCache(JOB_STATE_CACHE);

        String serverAddressStr = null;

        // Coordinate server address using IgniteCache before forking the clean JVM
        if (rank == 0) {
            final String serverHostname = NetworkUtil.getLocalAddress().getHostAddress();
            final int serverPort = NetworkUtil.findAvailableTcpPort();
            serverAddressStr = serverHostname + ":" + serverPort;
            cache.put(KEY_SERVER_ADDRESS, serverAddressStr);
        } else if (rank == 1) {
            serverAddressStr = waitForServerAddress(cache);
        } else {
            throw UnknownArgumentException.newInstance(int.class, rank);
        }

        // Prepare a temporary file to capture the logs from the forked JVM
        final File tempLogFile = File.createTempFile("ignite-task-", ".log");

        try {
            final Resource jobJarResource = Resources.LOADER.getResource(jobJar);
            final File localJobJar = new File(ContextProperties.TEMP_DIRECTORY, jobJarResource.getFilename());
            IOUtils.copy(jobJarResource.getInputStream(), new FileOutputStream(localJobJar));

            // Use the passed job JAR path directly via ForkJobHelper
            new ForkProcessHelper().fork(localJobJar, BootstrappedIgnite2TaskMain.class,
                    new String[] { "--rank", String.valueOf(rank), "--size", String.valueOf(size), "--serverAddress",
                            serverAddressStr, "--tempFile", tempLogFile.getAbsolutePath() });

            // Read the output written by the forked JVM
            final String logContent = Files.readFileToString(tempLogFile, StandardCharsets.UTF_8);

            final String taskClassName = (rank == 0) ? "LatencyServerTask" : "LatencyClientTask";
            final String logFileName = (rank + 1) + "_" + size + "_" + taskClassName + ".log";

            return new TaskResult(logFileName, logContent);
        } finally {
            if (rank == 0) {
                cache.remove(KEY_SERVER_ADDRESS);
            }
            Files.deleteQuietly(tempLogFile);
        }
    }

    private String waitForServerAddress(final IgniteCache<String, String> cache) {
        final Instant start = new Instant();
        while (!cache.containsKey(KEY_SERVER_ADDRESS)) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(50);
            if (start.isGreaterThan(ContextProperties.DEFAULT_NETWORK_TIMEOUT)) {
                throw new RuntimeException("Timeout waiting for server address in IgniteCache");
            }
        }
        return cache.get(KEY_SERVER_ADDRESS);
    }

    public static class TaskResult implements Serializable {
        private final String logFileName;
        private final String logContent;

        public TaskResult(final String logFileName, final String logContent) {
            this.logFileName = logFileName;
            this.logContent = logContent;
        }

        public String getLogFileName() {
            return logFileName;
        }

        public String getLogContent() {
            return logContent;
        }
    }
}