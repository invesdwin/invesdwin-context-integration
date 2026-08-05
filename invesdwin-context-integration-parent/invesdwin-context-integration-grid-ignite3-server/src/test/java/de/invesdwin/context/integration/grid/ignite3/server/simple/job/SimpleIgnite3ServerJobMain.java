package de.invesdwin.context.integration.grid.ignite3.server.simple.job;

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
import org.apache.ignite.deployment.DeploymentUnit;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.context.integration.grid.ignite3.Ignite3RestHelper;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.date.millis.FDateMillis;

@NotThreadSafe
public class SimpleIgnite3ServerJobMain extends AMain {

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

    @Option(name = "-j", aliases = "--jobJar", usage = "Defines the job JAR path", required = false)
    protected String jobJar;

    public SimpleIgnite3ServerJobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        final Path workDir = Path.of(ContextProperties.getCacheDirectory().getAbsolutePath(),
                "ignite3-work-" + nodeName, String.valueOf(FDateMillis.nowMillis()));

        // Configuration using correct multicast discovery fields directly under nodeFinder
        final int restPort = 10300 + (port - 3344);
        final String config = "ignite {\n" //
                + "  network: {\n" //
                + "    port: " + port + ",\n" //
                + "    listenAddresses: [ \"127.0.0.1\" ],\n" //
                + "    nodeFinder: {\n" //
                + "      type: \"MULTICAST\",\n" //
                + "      group: \"239.192.0.0\",\n" //
                + "      port: 47401\n" //
                + "    }\n" //
                + "  },\n" //
                + "  rest: {\n" //
                + "    port: " + restPort + "\n" //
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
                                "CREATE TABLE IF NOT EXISTS ignite3JobStateCache (key VARCHAR PRIMARY KEY, val VARCHAR)");

                final String unitId = "simple-node-unit";
                final String unitVersion = "3.1.0";

                if (jobJar != null) {
                    final String restAddress = "127.0.0.1:" + restPort;
                    Ignite3RestHelper.deployUnitViaRest(restAddress, unitId, unitVersion, jobJar);
                }

                final JobDescriptor.Builder<String, SimpleIgnite3ServerTask.TaskResult> descriptorBuilder = JobDescriptor.<String, SimpleIgnite3ServerTask.TaskResult> builder(
                        SimpleIgnite3ServerTask.class.getName()).resultClass(SimpleIgnite3ServerTask.TaskResult.class);

                if (jobJar != null) {
                    descriptorBuilder.units(List.of(new DeploymentUnit(unitId, unitVersion)));
                }

                final JobDescriptor<String, SimpleIgnite3ServerTask.TaskResult> descriptor = descriptorBuilder.build();

                final JobTarget target = JobTarget.anyNode(ignite.cluster().nodes());
                final List<CompletableFuture<SimpleIgnite3ServerTask.TaskResult>> futures = new ArrayList<>();

                for (int rank = 0; rank < size; rank++) {
                    final String args = rank + ";" + size;
                    final CompletableFuture<SimpleIgnite3ServerTask.TaskResult> future = ignite.compute()
                            .submitAsync(target, descriptor, args)
                            .thenCompose(org.apache.ignite.compute.JobExecution::resultAsync);
                    futures.add(future);
                }

                for (final CompletableFuture<SimpleIgnite3ServerTask.TaskResult> future : futures) {
                    final SimpleIgnite3ServerTask.TaskResult result = future.join();
                    final File logFile = new File(logDir, result.getLogFileName());
                    de.invesdwin.util.lang.Files.writeStringToFile(logFile, result.getLogContent(),
                            StandardCharsets.UTF_8);
                }
            } else {
                // Keep the worker process alive until it receives a shutdown signal from the test
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
        new SimpleIgnite3ServerJobMain(args).run();
    }
}