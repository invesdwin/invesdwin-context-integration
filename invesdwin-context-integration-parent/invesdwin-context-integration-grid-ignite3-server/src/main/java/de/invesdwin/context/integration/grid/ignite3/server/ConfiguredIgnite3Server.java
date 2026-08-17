package de.invesdwin.context.integration.grid.ignite3.server;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite3.instance.AConfiguredIgnite3Instance;
import de.invesdwin.context.integration.grid.ignite3.instance.ConfiguredServerAddressFinder;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.lang.Files;

@ThreadSafe
public final class ConfiguredIgnite3Server extends AConfiguredIgnite3Instance<IgniteServer> {

    private static final Log LOG = new Log(ConfiguredIgnite3Server.class);

    @Override
    protected IgniteServer startIgniteServer() {
        final String nodeName = "ignite-server-" + IntegrationProperties.HOSTNAME;

        // 1. Resolve addresses from service discovery
        final ConfiguredServerAddressFinder addressFinder = new ConfiguredServerAddressFinder();
        final String[] seedNodes = addressFinder.getAddresses();
        final String seedNodesHoconList = seedNodes.length > 0 ? "\"" + String.join("\", \"", seedNodes) + "\"" : "";

        // 2. Derive deterministic Meta Storage node names from discovered hostnames
        final List<String> metaStorageNodes = Arrays.stream(seedNodes)
                .map(address -> address.contains(":") ? address.substring(0, address.indexOf(':')) : address)
                .map(host -> "ignite-server-" + host)
                .collect(Collectors.toList());

        // Fallback: If discovery is completely empty, use the local node to bootstrap a 1-node cluster
        if (metaStorageNodes.isEmpty()) {
            metaStorageNodes.add(nodeName);
        }

        // 3. Build the properly nested HOCON configuration
        final String hoconConfig = "{\n" //
                + "  ignite: {\n" //
                + "    network: {\n" //
                + "      port: " + Ignite3ServerProperties.SERVER_COMMUNICATION_PORT + ",\n" //
                + "      nodeFinder: {\n" //
                + "        type: \"STATIC\",\n" //
                + "        netClusterNodes: [" + seedNodesHoconList + "]\n" //
                + "      }\n" //
                + "    },\n" //
                + "    clientConnector: { port: " + Ignite3ServerProperties.THIN_CLIENT_PORT + " },\n" //
                + "    rest: { port: " + Ignite3ServerProperties.REST_PORT + " }\n" //
                + "  }\n" //
                + "}";

        // 4. Clean up stale work directory state to prevent validation errors
        final Path workDir = Path.of(ContextProperties.getCacheDirectory().getAbsolutePath(),
                "ignite3-work-" + nodeName);
        final Path configFile = workDir.resolve("ignite-config.conf");

        try {
            final File workDirFile = workDir.toFile();
            if (workDirFile.exists()) {
                Files.deleteDirectory(workDirFile);
            }
            java.nio.file.Files.createDirectories(workDir);
            java.nio.file.Files.writeString(configFile, hoconConfig, StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create Ignite configuration file", e);
        }

        // 5. Start the server
        final IgniteServer server = IgniteServer.start(nodeName, configFile, workDir);

        // 6. Initialize the cluster programmatically
        final InitParameters initParameters = InitParameters.builder()
                .metaStorageNodeNames(metaStorageNodes)
                .clusterName("invesdwin-cluster")
                .build();

        try {
            server.initCluster(initParameters);
        } catch (final Exception e) {
            // Ignored. If multiple nodes boot concurrently, the first one to execute this
            // will successfully initialize the cluster. Subsequent calls will throw an
            // "already initialized" exception which is perfectly safe to swallow.
        }

        return server;
    }

    @Override
    protected Ignite getApi(final IgniteServer server) {
        return server.api();
    }

    @Override
    protected void shutdown(final IgniteServer server) {
        server.shutdown();
    }
}