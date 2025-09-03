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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaBridges;
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public class RedpandaConnectBridges implements IKafkaBridges {

    private final IKafkaContainer<?> kafkaContainer;
    private final List<GenericContainer<?>> bridges = new ArrayList<>();

    public RedpandaConnectBridges(final IKafkaContainer<?> kafkaContainer) {
        this.kafkaContainer = kafkaContainer;
    }

    //CHECKSTYLE:OFF
    @Override
    public IKafkaBridges withNetwork(final Network network) {
        return this;
    }

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

    @Override
    public GenericContainer<?> startBridge(final String inputTopic, final String outputTopic) {
        final String uuid = java.util.UUID.randomUUID().toString();

        final String template = readResourceAsString("connect-bridge.yaml");
        final String yamlConfig = template
                .replace("${BOOTSTRAP}", Strings.removeStart(kafkaContainer.getBootstrapServers(), "PLAINTEXT://"))
                .replace("${IN_TOPIC}", inputTopic)
                .replace("${OUT_TOPIC}", outputTopic)
                .replace("${UUID}", uuid);

        @SuppressWarnings("resource")
        final GenericContainer<?> bridgeContainer = new GenericContainer<>(
                DockerImageName.parse("docker.redpanda.com/redpandadata/connect:4"))
                        .withNetworkMode("host")
                        .withCopyToContainer(Transferable.of(yamlConfig.getBytes(StandardCharsets.UTF_8)),
                                "/connect.yaml")
                        .withCommand("run", "/connect.yaml")
                        .waitingFor(Wait.forLogMessage(".*Listening for HTTP requests at: http://0.0.0.0:4195.*", 1));
        //.waitingFor(Wait.forLogMessage(".*Input type .* is now active.*", 1));

        bridgeContainer.start();
        bridges.add(bridgeContainer);
        return bridgeContainer;
    }

    private static String readResourceAsString(final String nameInSamePackage) {
        final URL url = RedpandaConnectBridges.class.getResource(nameInSamePackage);
        try (InputStream in = url.openStream()) {
            return IOUtils.toString(in, Charset.defaultCharset());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
