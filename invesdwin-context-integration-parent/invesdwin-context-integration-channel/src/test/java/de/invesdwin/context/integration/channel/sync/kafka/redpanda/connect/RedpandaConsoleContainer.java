package de.invesdwin.context.integration.channel.sync.kafka.redpanda.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.images.builder.traits.BuildContextBuilderTrait;
import org.testcontainers.images.builder.traits.StringsTrait;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.log.Log;

// TODO: make this a separate container only for redpanda connect, which then works both with kafka and redpanda and
// confluent
// TODO: remove host free
// TODO: remove waitForHostPortToBeFree
// TODO: move start method to consructor
@NotThreadSafe
public class RedpandaConsoleContainer extends GenericContainer<RedpandaConsoleContainer>
        implements BuildContextBuilderTrait<RedpandaConsoleContainer>, StringsTrait<RedpandaConsoleContainer> {

    private static final int CONSOLE_PORT = 8080;
    private final Log log = new Log(this);
    private final IKafkaContainer<?> kafkaContainer;

    public RedpandaConsoleContainer(final IKafkaContainer<?> kafkaContainer) {
        super("docker.redpanda.com/redpandadata/console:v3.2.0");
        dependsOn(kafkaContainer);
        this.kafkaContainer = kafkaContainer;
    }

    @Override
    protected void configure() {
        super.configure();
        withExposedPorts(CONSOLE_PORT);
        final StringBuilder configYml = new StringBuilder("kafka:");
        configYml.append("\n  brokers: [\"");
        configYml.append(kafkaContainer.getBootstrapServers());
        configYml.append("\"]");
        final String configFile = "/tmp/config.yml";
        withFileFromString(configFile, configYml.toString());

        setCommand("/bin/sh", "-c \"echo \\\"" + configFile + "\\\" > /tmp/config.yml; /app/console\"");
    }

    @Override
    public void start() {
        super.start();
        log.info("Redpanda console website: %s", getRedpandaConsoleAddress());
    }

    //CHECKSTYLE:OFF
    @Override
    public RedpandaConsoleContainer withFileFromTransferable(final String path, final Transferable transferable) {
        //CHECKSTYLE:ON
        return super.withCopyToContainer(transferable, path);
    }

    public String getRedpandaConsoleAddress() {
        //CHECKSTYLE:OFF
        return String.format("http://%s:%s", getHost(), getMappedPort(CONSOLE_PORT));
        //CHECKSTYLE:ON
    }
}
