package de.invesdwin.context.integration.grid.ignite3.server;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.integration.grid.ignite3.instance.AConfiguredIgnite3Instance;
import de.invesdwin.context.integration.grid.ignite3.instance.ConfiguredServerAddressFinder;
import de.invesdwin.util.lang.Files;

@ThreadSafe
public final class ConfiguredIgnite3Server extends AConfiguredIgnite3Instance<IgniteServer> {

    @Override
    protected IgniteServer startIgniteServer() {
        final String nodeName = "ignite-server-" + IntegrationProperties.HOSTNAME;

        // Use ConfiguredServerAddressFinder to dynamically resolve addresses from the registry[cite: 57]
        final ConfiguredServerAddressFinder addressFinder = new ConfiguredServerAddressFinder();
        final String[] seedNodes = addressFinder.getAddresses();
        final String seedNodesHoconList = seedNodes.length > 0 ? "\"" + String.join("\", \"", seedNodes) + "\"" : "";

        // Build the HOCON configuration dynamically with the resolved seed nodes
        final String hoconConfig = "{\n" //
                + "  network: {\n" //
                + "    port: " + Ignite3ServerProperties.SERVER_COMMUNICATION_PORT + ",\n" //
                + "    nodeFinder: {\n" //
                + "      type: \"STATIC\",\n" //
                //we need at least 3 stable seed nodes to form a cluster from which others can dynamically join or exit without causing any issues
                + "      netClusterNodes: [" + seedNodesHoconList + "]\n" //
                + "    }\n" //
                + "  },\n" //
                + "  clientConnector: { port: " + Ignite3ServerProperties.THIN_CLIENT_PORT + " },\n" //
                + "  rest: { port: " + Ignite3ServerProperties.REST_PORT + " }\n" //
                + "}";

        final Path workDir = Path.of(ContextProperties.getCacheDirectory().getAbsolutePath(),
                "ignite3-work-" + nodeName);
        final Path configFile = workDir.resolve("ignite-config.conf");
        try {
            Files.createDirectories(workDir);
            Files.writeString(configFile, hoconConfig, StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create Ignite configuration file", e);
        }

        return IgniteServer.start(nodeName, configFile, workDir);
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