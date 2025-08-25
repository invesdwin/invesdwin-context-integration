package de.invesdwin.context.integration.channel.sync.kafka.redpanda.connect;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.log.Log;

// TODO: extend DockerComposeContainer, not GenericContainer
// TODO: make this a separate container only for redpanda connect, which then works both with kafka and redpanda and
// confluent
@NotThreadSafe
public class RedpandaConnectBridges implements Startable {
    private final Log log = new Log(this);

    private final List<GenericContainer<?>> bridges = new ArrayList<>();

    @Override
    public void start() {}

    @Override
    public void stop() {
        for (final GenericContainer<?> bridge : bridges) {
            try {
                bridge.stop();
            } catch (final Exception ignore) {
            }
        }
        bridges.clear();
    }

    public GenericContainer<?> startBridge(final String inputTopic, final String outputTopic) throws IOException {
        final String bootstrap = getBootstrapServers();
        final String uuid = java.util.UUID.randomUUID().toString();

        final String template = readResourceAsString("connect-bridge.yaml");
        final String yamlConfig = template.replace("${BOOTSTRAP}", bootstrap)
                .replace("${IN_TOPIC}", inputTopic)
                .replace("${OUT_TOPIC}", outputTopic)
                .replace("${UUID}", uuid);

        @SuppressWarnings("resource")
        final GenericContainer<?> bridgeContainer = new GenericContainer<>(
                DockerImageName.parse("docker.redpanda.com/redpandadata/connect:latest"))
                        .withNetworkMode("host")
                        .withCopyToContainer(Transferable.of(yamlConfig.getBytes(StandardCharsets.UTF_8)),
                                "/connect.yaml")
                        .withCommand("run", "/connect.yaml")
                        .waitingFor(Wait.forLogMessage(".*Input type .* is now active.*", 1));

        bridgeContainer.start();
        bridges.add(bridgeContainer);
        return bridgeContainer;
    }

    private static String getBootstrapServers() {
        return RedpandaConsoleContainer.getBootstrapServers();
    }

    private static String readResourceAsString(final String nameInSamePackage) throws IOException {
        final URL url = RedpandaConnectBridges.class.getResource(nameInSamePackage);
        try (InputStream in = url.openStream()) {
            return IOUtils.toString(in, Charset.defaultCharset());
        }
    }

}
