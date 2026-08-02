package de.invesdwin.context.integration.grid.ignite3.node.simple.job;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.millis.FDateMillis;

@NotThreadSafe
public class Ignite3LocalJobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;

    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;

    public Ignite3LocalJobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        final String nodeName = "embedded-node";

        // Ensure a clean working directory for each run
        final Path workDir = Path.of(ContextProperties.getCacheDirectory().getAbsolutePath(), "ignite3-work",
                String.valueOf(FDateMillis.nowMillis()));

        // Configuration wrapped inside the required 'ignite' root block
        final String config = "ignite {\n" + "  network: {\n" + "    port: 3344,\n" + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\" ]\n" + "    }\n" + "  },\n"
                + "  clientConnector: { port: 10800 }\n" + "}";

        IgniteServer server = null;
        try {
            final Path configFile = workDir.resolve("ignite-config.conf");
            Files.createDirectories(workDir);
            Files.writeString(configFile, config, StandardCharsets.UTF_8);

            // 1. Start Server Node using IgniteServer with the file Path
            server = IgniteServer.start(nodeName, configFile, workDir);

            // 2. Initialize cluster using InitParameters
            final InitParameters initParameters = InitParameters.builder()
                    .metaStorageNodeNames(List.of(nodeName))
                    .clusterName("local-cluster")
                    .build();
            server.initCluster(initParameters);

            // 3. Get the Ignite API instance
            final Ignite ignite = server.api();

            // 4. Create required caches/tables
            ignite.sql()
                    .execute(null,
                            "CREATE TABLE IF NOT EXISTS ignite3JobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

            // 5. Submit Job directly
            final JobDescriptor<String, Ignite3Task.TaskResult> descriptor = JobDescriptor.<String, Ignite3Task.TaskResult> builder(
                    Ignite3Task.class.getName()).resultClass(Ignite3Task.TaskResult.class).build();

            final JobTarget target = JobTarget.anyNode(ignite.clusterNodes());
            final List<CompletableFuture<Ignite3Task.TaskResult>> futures = new ArrayList<>();

            for (int rank = 0; rank < size; rank++) {
                final String args = rank + ";" + size;
                final CompletableFuture<Ignite3Task.TaskResult> future = ignite.compute()
                        .submitAsync(target, descriptor, args)
                        .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);
                futures.add(future);
            }

            // 6. Save output logs
            for (final CompletableFuture<Ignite3Task.TaskResult> future : futures) {
                final Ignite3Task.TaskResult result = future.join();
                final File logFile = new File(logDir, result.getLogFileName());
                de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(), StandardCharsets.UTF_8);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to run embedded Ignite 3 job", e);
        } finally {
            // 7. Tear down the server cleanly
            if (server != null) {
                server.shutdown();
            }
        }
    }

    public static void main(final String[] args) {
        new Ignite3LocalJobMain(args).run();
    }
}