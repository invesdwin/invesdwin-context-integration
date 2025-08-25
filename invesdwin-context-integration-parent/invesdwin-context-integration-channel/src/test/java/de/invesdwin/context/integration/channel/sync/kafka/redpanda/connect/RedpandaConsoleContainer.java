package de.invesdwin.context.integration.channel.sync.kafka.redpanda.connect;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.images.builder.traits.BuildContextBuilderTrait;
import org.testcontainers.images.builder.traits.StringsTrait;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.model.ContainerNetwork;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.time.duration.Duration;

// TODO: extend DockerComposeContainer, not GenericContainer
// TODO: make this a separate container only for redpanda connect, which then works both with kafka and redpanda and
// confluent
// TODO: remove host free
// TODO: remove waitForHostPortToBeFree
// TODO: move start method to consructor
@SuppressWarnings("deprecation")
@NotThreadSafe
public class RedpandaConsoleContainer extends GenericContainer<RedpandaConsoleContainer>
        implements BuildContextBuilderTrait<RedpandaConsoleContainer>, StringsTrait<RedpandaConsoleContainer> {
    private static final File COMPOSE_FILE = getComposeFileFromClasspath();
    private static final int[] PORTS = new int[] { 9092, 8080 }; //NetworkUtil.findAvailableTcpPorts(2);
    private static final int KAFKA_PORT = PORTS[0];
    private final Log log = new Log(this);
    private final DockerComposeContainer<?> compose;
    private final List<GenericContainer<?>> bridges = new ArrayList<>();
    private final int consolePort = PORTS[1];

    @SuppressWarnings("resource")
    public RedpandaConsoleContainer() {

        //TODO: use NetworkUtil to find random ports to make this test not collide with others when run in parallel

        compose = new DockerComposeContainer<>(COMPOSE_FILE).withEnv("KAFKA_PORT", String.valueOf(KAFKA_PORT))
                .withEnv("CONSOLE_PORT", String.valueOf(consolePort))
                .withExposedService("redpanda", 9092,
                        Wait.forListeningPort().withStartupTimeout(Duration.TWO_MINUTES.javaTimeValue()))
                .withExposedService("console", 8080,
                        Wait.forListeningPort().withStartupTimeout(Duration.TWO_MINUTES.javaTimeValue()));

        compose.start();

        final ContainerState containerState = compose.getContainerByServiceName("redpanda").get();
        final ContainerNetwork network = containerState.getContainerInfo()
                .getNetworkSettings()
                .getNetworks()
                .values()
                .iterator()
                .next();
        final String containerIp = network.getIpAddress();
        final String gateway = network.getGateway();

        //TODO: create the config file inn memory without docker-compose, directly with GenericContainer
        //withFileFromString(INTERNAL_HOST_HOSTNAME, STATE_HEALTHY);
        log.info("Redpanda Console: %s", getConsoleUrl());
        log.info("Kafka Bootstrap Servers: %s", getBootstrapServers());
    }

    private static File getComposeFileFromClasspath() {
        try {
            return new File(RedpandaConsoleContainer.class.getResource("docker-compose-rp.yaml").toURI());
        } catch (final URISyntaxException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        for (final GenericContainer<?> bridge : bridges) {
            try {
                bridge.stop();
            } catch (final Exception ignore) {
            }
        }
        bridges.clear();

        if (compose != null) {
            compose.stop();
        }

    }

    public static String getBootstrapServers() {
        return "172.28.0.1:" + KAFKA_PORT;
    }

    public String getConsoleUrl() {
        return "http://localhost:" + consolePort;
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
                        //                        .waitingFor(Wait.forLogMessage(".*Input type kafka_franz is now active.*", 1)
                        .withStartupTimeout(Duration.ONE_MINUTE.javaTimeValue());

        bridgeContainer.start();
        bridges.add(bridgeContainer);
        return bridgeContainer;
    }

    private static String readResourceAsString(final String nameInSamePackage) throws IOException {
        final URL url = RedpandaConsoleContainer.class.getResource(nameInSamePackage);
        try (InputStream in = url.openStream()) {
            return IOUtils.toString(in, Charset.defaultCharset());
        }
    }

    //CHECKSTYLE:OFF
    @Override
    public RedpandaConsoleContainer withFileFromTransferable(final String path, final Transferable transferable) {
        //CHECKSTYLE:ON
        //super.copyFileToContainer(transferable, path);
        return this;
    }
}
