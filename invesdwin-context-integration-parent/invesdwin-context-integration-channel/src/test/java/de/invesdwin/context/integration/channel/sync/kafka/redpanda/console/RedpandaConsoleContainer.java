package de.invesdwin.context.integration.channel.sync.kafka.redpanda.console;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.GenericContainer;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public class RedpandaConsoleContainer extends GenericContainer<RedpandaConsoleContainer> {

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
        final String g = Strings.removeStart(kafkaContainer.getBootstrapServers(), "PLAINTEXT://");
        withEnv("KAFKA_BROKERS", g);
    }

    @Override
    public void start() {
        super.start();
        log.info("Redpanda console website: %s", getRedpandaConsoleAddress());
    }

    public String getRedpandaConsoleAddress() {
        //CHECKSTYLE:OFF
        return String.format("http://%s:%s", getHost(), getMappedPort(CONSOLE_PORT));
        //CHECKSTYLE:ON
    }
}
