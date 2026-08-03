package de.invesdwin.context.integration.grid.ignite3.client.bootstrapped.job;

import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.table.KeyValueView;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.jar.ForkJobHelper;
import de.invesdwin.context.integration.network.NetworkUtil;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.error.UnknownArgumentException;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class BootstrappedIgnite3ClientTask implements ComputeJob<String, BootstrappedIgnite3ClientTask.TaskResult> {

    public static final String JOB_STATE_CACHE = "jobStateCache";
    public static final String KEY_SERVER_ADDRESS = "serverAddress";

    @Override
    public CompletableFuture<TaskResult> executeAsync(final JobExecutionContext context, final String arg) {
        return CompletableFuture.supplyAsync(() -> {
            final String[] parts = arg.split(";");
            final int rank = Integer.parseInt(parts[0]);
            final int size = Integer.parseInt(parts[1]);

            try {
                // Access cache directly via embedded node access context
                final KeyValueView<String, String> cache = context.ignite()
                        .tables()
                        .table(JOB_STATE_CACHE)
                        .keyValueView(String.class, String.class);

                String serverAddressStr = null;

                if (rank == 0) {
                    final String serverHostname = NetworkUtil.getLocalAddress().getHostAddress();
                    final int serverPort = NetworkUtil.findAvailableTcpPort();
                    serverAddressStr = serverHostname + ":" + serverPort;
                    cache.put(null, KEY_SERVER_ADDRESS, serverAddressStr);
                } else if (rank == 1) {
                    serverAddressStr = waitForServerAddress(cache);
                } else {
                    throw UnknownArgumentException.newInstance(int.class, rank);
                }

                final File tempLogFile = File.createTempFile("ignite-task-", ".log");

                try {
                    ForkJobHelper.fork(
                            new File(BootstrappedIgnite3ClientTask.class.getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI()),
                            BootstrappedIgnite3ClientTaskMain.class,
                            new String[] { "--rank", String.valueOf(rank), "--size", String.valueOf(size),
                                    "--serverAddress", serverAddressStr, "--tempFile", tempLogFile.getAbsolutePath() });

                    final String logContent = Files.readFileToString(tempLogFile, StandardCharsets.UTF_8);
                    final String taskClassName = (rank == 0) ? "LatencyServerTask" : "LatencyClientTask";
                    final String logFileName = (rank + 1) + "_" + size + "_" + taskClassName + ".log";

                    return new TaskResult(logFileName, logContent);
                } finally {
                    if (rank == 0) {
                        cache.remove(null, KEY_SERVER_ADDRESS);
                    }
                    Files.deleteQuietly(tempLogFile);
                }
            } catch (final Exception e) {
                throw Err.process(new RuntimeException("Failed to execute forked Ignite 3 task", e));
            }
        });
    }

    private String waitForServerAddress(final KeyValueView<String, String> cache) {
        final Instant start = new Instant();
        while (!cache.contains(null, KEY_SERVER_ADDRESS)) {
            FTimeUnit.MILLISECONDS.sleepNoInterrupt(50);
            if (start.isGreaterThan(ContextProperties.DEFAULT_NETWORK_TIMEOUT)) {
                throw new RuntimeException("Timeout waiting for server address in Ignite 3 KV view");
            }
        }
        return cache.get(null, KEY_SERVER_ADDRESS);
    }

    public static class TaskResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private String logFileName;
        private String logContent;

        public TaskResult() {}

        public TaskResult(final String logFileName, final String logContent) {
            this.logFileName = logFileName;
            this.logContent = logContent;
        }

        public String getLogFileName() {
            return logFileName;
        }

        public void setLogFileName(final String logFileName) {
            this.logFileName = logFileName;
        }

        public String getLogContent() {
            return logContent;
        }

        public void setLogContent(final String logContent) {
            this.logContent = logContent;
        }
    }
}