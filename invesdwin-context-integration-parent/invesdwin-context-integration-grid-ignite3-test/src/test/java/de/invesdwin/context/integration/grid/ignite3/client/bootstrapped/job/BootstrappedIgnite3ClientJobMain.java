package de.invesdwin.context.integration.grid.ignite3.client.bootstrapped.job;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.integration.grid.ignite3.Ignite3RestHelper;
import de.invesdwin.util.lang.string.Charsets;

@NotThreadSafe
public class BootstrappedIgnite3ClientJobMain extends AMain {

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-m", aliases = "--master", usage = "Defines the Ignite master address (client port)", required = true)
    protected String master;
    @Option(name = "--rest", usage = "Defines the Ignite REST address", required = true)
    protected String restAddress;
    @Option(name = "-j", aliases = "--jobJar", usage = "Defines the job JAR path", required = true)
    protected String jobJar;

    public BootstrappedIgnite3ClientJobMain(final String[] args) {
        super(args, true);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        try {
            final String unitId = "fork-job-unit";
            final String unitVersion = "3.0.0";
            Ignite3RestHelper.deployUnitViaRest(restAddress, unitId, unitVersion, jobJar);

            final DeploymentUnit unit = new DeploymentUnit(unitId, unitVersion);

            try (IgniteClient client = IgniteClient.builder().addresses(master).build()) {
                client.sql()
                        .execute(null,
                                "CREATE TABLE IF NOT EXISTS jobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

                final JobDescriptor<String, BootstrappedIgnite3ClientTask.TaskResult> descriptor = JobDescriptor.<String, BootstrappedIgnite3ClientTask.TaskResult> builder(
                        BootstrappedIgnite3ClientTask.class.getName())
                        .resultClass(BootstrappedIgnite3ClientTask.TaskResult.class)
                        .units(List.of(unit))
                        .build();

                final JobTarget target = JobTarget.anyNode(client.cluster().nodes());
                final List<CompletableFuture<BootstrappedIgnite3ClientTask.TaskResult>> futures = new ArrayList<>();

                for (int rank = 0; rank < size; rank++) {
                    final String args = rank + ";" + size + ";" + jobJar;
                    final CompletableFuture<BootstrappedIgnite3ClientTask.TaskResult> future = client.compute()
                            .submitAsync(target, descriptor, args)
                            .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);

                    futures.add(future);
                }

                for (final CompletableFuture<BootstrappedIgnite3ClientTask.TaskResult> future : futures) {
                    final BootstrappedIgnite3ClientTask.TaskResult result = future.join();
                    final File logFile = new File(logDir, result.getLogFileName());
                    de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(),
                            Charsets.defaultCharset());
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to run Ignite 3 job", e);
        }
    }

    public static void main(final String[] args) {
        new BootstrappedIgnite3ClientJobMain(args).run();
    }
}