package de.invesdwin.context.integration.grid.ignite3.node.bootstrapped.job;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

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
public class ForkIgnite3JobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = false)
    protected String logDir;

    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = false)
    protected int size = 2;

    @Option(name = "-n", aliases = "--nodeName", usage = "Defines the ignite node name", required = true)
    protected String nodeName;

    @Option(name = "-p", aliases = "--port", usage = "Defines the ignite network port", required = true)
    protected int port;

    @Option(name = "-m", aliases = "--master", usage = "Defines if this node submits the job", required = false)
    protected boolean master = false;

    public ForkIgnite3JobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        final Path workDir = Path.of(ContextProperties.getCacheDirectory().getAbsolutePath(),
                "ignite3-work-" + nodeName, String.valueOf(FDateMillis.nowMillis()));

        // Configuration mapping network, REST, and client connector ports dynamically to avoid collisions
        final String config = "ignite {\n" //
                + "  network: {\n" //
                + "    port: " + port + ",\n" //
                + "    listenAddresses: [ \"127.0.0.1\" ],\n" //
                + "    nodeFinder: {\n" //
                + "      netClusterNodes: [ \"127.0.0.1:3344\", \"127.0.0.1:3345\" ]\n" //
                + "    }\n" //
                + "  },\n" //
                + "  rest: {\n" //
                + "    port: " + (10300 + (port - 3344)) + "\n" //
                + "  },\n" //
                + "  clientConnector: {\n" //
                + "    port: " + (10800 + (port - 3344)) + "\n" //
                + "  }\n" //
                + "}";

        IgniteServer server = null;
        try {
            final Path configFile = workDir.resolve("ignite-config.conf");
            Files.createDirectories(workDir);
            Files.writeString(configFile, config, StandardCharsets.UTF_8);

            server = IgniteServer.start(nodeName, configFile, workDir);

            if (master) {
                final InitParameters initParameters = InitParameters.builder()
                        .metaStorageNodeNames(List.of(nodeName))
                        .clusterName("local-cluster")
                        .build();
                server.initCluster(initParameters);

                final Ignite ignite = server.api();

                ignite.sql()
                        .execute(null,
                                "CREATE TABLE IF NOT EXISTS jobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

                final JobDescriptor<String, ForkIgnite3Task.TaskResult> descriptor = JobDescriptor.<String, ForkIgnite3Task.TaskResult> builder(
                        ForkIgnite3Task.class.getName()).resultClass(ForkIgnite3Task.TaskResult.class).build();

                final JobTarget target = JobTarget.anyNode(ignite.clusterNodes());
                final List<CompletableFuture<ForkIgnite3Task.TaskResult>> futures = new ArrayList<>();

                for (int rank = 0; rank < size; rank++) {
                    final String args = rank + ";" + size;
                    final CompletableFuture<ForkIgnite3Task.TaskResult> future = ignite.compute()
                            .submitAsync(target, descriptor, args)
                            .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);
                    futures.add(future);
                }

                for (final CompletableFuture<ForkIgnite3Task.TaskResult> future : futures) {
                    final ForkIgnite3Task.TaskResult result = future.join();
                    final File logFile = new File(logDir, result.getLogFileName());
                    de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(),
                            StandardCharsets.UTF_8);
                }
            } else {
                final CountDownLatch latch = new CountDownLatch(1);
                final IgniteServer srvRef = server;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    latch.countDown();
                    if (srvRef != null) {
                        srvRef.shutdown();
                    }
                }));
                latch.await();
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to run embedded Ignite 3 node " + nodeName, e);
        } finally {
            if (master && server != null) {
                server.shutdown();
            }
        }
    }

    public static void main(final String[] args) {
        new ForkIgnite3JobMain(args).run();
    }
}