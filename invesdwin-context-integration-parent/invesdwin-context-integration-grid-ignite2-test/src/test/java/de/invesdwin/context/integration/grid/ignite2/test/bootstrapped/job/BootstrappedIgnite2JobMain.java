package de.invesdwin.context.integration.grid.ignite2.test.bootstrapped.job;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.PlatformInitializerProperties;
import de.invesdwin.context.beans.init.AMain;
import de.invesdwin.context.beans.init.platform.util.AspectJWeaverIncludesConfigurer;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public class BootstrappedIgnite2JobMain extends AMain {

    private static final boolean BOOTSTRAP = true;

    static {
        AspectJWeaverIncludesConfigurer.setShowWeaveInfo(false);
        PlatformInitializerProperties.setAllowed(BOOTSTRAP);
    }

    @Option(name = "-l", aliases = "--logDir", usage = "Defines the log directory", required = true)
    protected String logDir;
    @Option(name = "-s", aliases = "--size", usage = "Defines the number of processes", required = true)
    protected int size;
    @Option(name = "-m", aliases = "--master", usage = "Defines the Ignite master address")
    protected String master;
    @Option(name = "-j", aliases = "--jobJar", usage = "Defines the job JAR path", required = true)
    protected String jobJar;

    public BootstrappedIgnite2JobMain() {
        super(Strings.EMPTY_ARRAY, BOOTSTRAP);
    }

    public BootstrappedIgnite2JobMain(final String[] args) {
        super(args, BOOTSTRAP);
    }

    @Override
    protected void startApplication(final CmdLineParser parser) {
        runIgniteJob(logDir, size, master, jobJar);
    }

    public static void runIgniteJob(final String logDir, final int size, final String master, final String jobJar) {
        final IgniteConfiguration cfg = new IgniteConfiguration();
        final File workDir = new File(ContextProperties.getCacheDirectory(), "ignite-work");
        cfg.setWorkDirectory(workDir.getAbsolutePath());
        cfg.setPeerClassLoadingEnabled(true);

        final TcpDiscoverySpi spi = new TcpDiscoverySpi();
        final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        if (master != null) {
            ipFinder.setAddresses(java.util.Collections.singletonList(master));
            cfg.setClientMode(true);
        } else {
            ipFinder.setAddresses(java.util.Collections.singletonList("127.0.0.1:47500..47509"));
        }

        spi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(spi);

        try (Ignite ignite = Ignition.start(cfg)) {
            final List<BootstrappedIgnite2Task> tasks = new ArrayList<>();
            for (int rank = 0; rank < size; rank++) {
                tasks.add(new BootstrappedIgnite2Task(rank, size, jobJar));
            }

            // Distribute and execute worker tasks across the compute grid
            final Collection<BootstrappedIgnite2Task.TaskResult> results = ignite.compute().call(tasks);

            // Write in-memory gathered logs to logDir
            if (logDir != null) {
                for (final BootstrappedIgnite2Task.TaskResult result : results) {
                    final File logFile = new File(logDir, result.getLogFileName());
                    try {
                        Files.writeStringToFile(logFile, result.getLogContent(), StandardCharsets.UTF_8);
                    } catch (final IOException e) {
                        throw new RuntimeException("Failed to write task log file: " + logFile, e);
                    }
                }
            }
        }
    }

    public static void main(final String[] args) {
        new BootstrappedIgnite2JobMain(args).run();
    }
}