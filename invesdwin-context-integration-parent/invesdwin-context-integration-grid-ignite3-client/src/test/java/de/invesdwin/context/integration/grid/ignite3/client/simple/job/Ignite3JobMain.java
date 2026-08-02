package de.invesdwin.context.integration.grid.ignite3.client.simple.job;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.context.integration.grid.ignite3.Ignite3RestHelper;

@NotThreadSafe
public class Ignite3JobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-m", aliases = "--master", usage = "Defines the Ignite master address", required = true)
    protected String master;
    @Option(name = "--rest", usage = "Defines the Ignite REST address", required = true)
    protected String restAddress;
    @Option(name = "-j", aliases = "--jobJar", usage = "Defines the job JAR path", required = true)
    protected String jobJar;

    public Ignite3JobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        try {
            final String unitId = "simple-job-unit";
            final String unitVersion = "3.1.0";
            Ignite3RestHelper.deployUnitViaRest(restAddress, unitId, unitVersion, jobJar);

            final DeploymentUnit unit = new DeploymentUnit(unitId, unitVersion);

            try (IgniteClient client = IgniteClient.builder().addresses(master).build()) {
                client.sql()
                        .execute(null,
                                "CREATE TABLE IF NOT EXISTS ignite3JobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

                final JobDescriptor<String, Ignite3Task.TaskResult> descriptor = JobDescriptor.<String, Ignite3Task.TaskResult> builder(
                        Ignite3Task.class.getName())
                        .resultClass(Ignite3Task.TaskResult.class)
                        .units(List.of(unit))
                        .build();

                final JobTarget target = JobTarget.anyNode(client.cluster().nodes());
                final List<CompletableFuture<Ignite3Task.TaskResult>> futures = new ArrayList<>();

                for (int rank = 0; rank < size; rank++) {
                    final String args = rank + ";" + size + ";" + master;
                    final CompletableFuture<Ignite3Task.TaskResult> future = client.compute()
                            .submitAsync(target, descriptor, args)
                            .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);
                    futures.add(future);
                }

                final File targetDir = parseLogDirectory(logDir);
                for (final CompletableFuture<Ignite3Task.TaskResult> future : futures) {
                    final Ignite3Task.TaskResult result = future.join();
                    final File logFile = new File(targetDir, result.getLogFileName());
                    de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(),
                            StandardCharsets.UTF_8);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to run Ignite 3 job", e);
        }
    }

    private static File parseLogDirectory(final String logDir) {
        if (logDir.startsWith("file:")) {
            return new File(URI.create(logDir));
        }
        return new File(logDir);
    }

    public static void main(final String[] args) {
        new Ignite3JobMain(args).run();
    }
}