package de.invesdwin.context.integration.channel.sync.kafka.redpanda.console;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.GenericContainer;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaConnectContainer;
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.ISchemaRegistryContainer;
import de.invesdwin.context.log.Log;

@NotThreadSafe
public class RedpandaConsoleContainer extends GenericContainer<RedpandaConsoleContainer> {

    private static final int CONSOLE_PORT = 8080;
    private final Log log = new Log(this);
    private final IKafkaContainer<?> kafkaContainer;
    private ISchemaRegistryContainer<?> schemaRegistryContainer;
    private IKafkaConnectContainer<?> kafkaConnectContainer;

    public RedpandaConsoleContainer(final IKafkaContainer<?> kafkaContainer) {
        super("docker.redpanda.com/redpandadata/console:v3.2.0");
        dependsOn(kafkaContainer);
        this.kafkaContainer = kafkaContainer;
    }

    @Override
    protected void configure() {
        super.configure();
        withExposedPorts(CONSOLE_PORT);
        /*
         * each setting from the config file is exposed as an env variable:
         * https://github.com/redpanda-data/console/blob/master/docs/config/console.yaml
         */
        withEnv("KAFKA_BROKERS", kafkaContainer.getBootstrapServers());
        if (schemaRegistryContainer != null) {
            withEnv("SCHEMAREGISTRY_ENABLED", "true");
            withEnv("SCHEMAREGISTRY_URLS", schemaRegistryContainer.getSchemaRegistryUrl());
        }
        if (kafkaConnectContainer != null) {
            withEnv("KAFKACONNECT_ENABLED", "true");
            withEnv("KAFKACONNECT_CLUSTERS_NAME", "local");
            withEnv("KAFKACONNECT_CLUSTERS_URL", kafkaConnectContainer.getKafkaConnectUrl());
        }
    }

    public void setSchemaRegistryContainer(final ISchemaRegistryContainer<?> schemaRegistryContainer) {
        dependsOn(schemaRegistryContainer);
        this.schemaRegistryContainer = schemaRegistryContainer;
    }

    public ISchemaRegistryContainer<?> getSchemaRegistryContainer() {
        return schemaRegistryContainer;
    }

    public void setKafkaConnectContainer(final IKafkaConnectContainer<?> kafkaConnectContainer) {
        dependsOn(kafkaConnectContainer);
        this.kafkaConnectContainer = kafkaConnectContainer;
    }

    public IKafkaConnectContainer<?> getKafkaConnectContainer() {
        return kafkaConnectContainer;
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
