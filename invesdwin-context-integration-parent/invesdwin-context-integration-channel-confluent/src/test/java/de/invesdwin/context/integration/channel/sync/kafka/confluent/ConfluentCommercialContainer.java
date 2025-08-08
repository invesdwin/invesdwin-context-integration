package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.containers.KafkaContainer;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer;

@NotThreadSafe
public class ConfluentCommercialContainer implements IKafkaContainer<KafkaContainer> {
    private final KafkaContainer kafka;

    private final SchemaRegistryContainer schemaRegistry;

    public ConfluentCommercialContainer(final boolean withSchemaRegistry) {
        final CPTestContainerFactory factory = new CPTestContainerFactory();

        this.kafka = factory.createConfluentServer();
        this.schemaRegistry = withSchemaRegistry ? factory.createSchemaRegistry(kafka) : null;

        if (schemaRegistry != null) {
            schemaRegistry.start();
        } else {
            kafka.start();
        }
    }

    @Override
    public void stop() {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        } else {
            kafka.stop();
        }
    }

    @Override
    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    public String getSchemaRegistryUrl() {
        if (schemaRegistry == null) {
            throw new IllegalStateException("Schema Registry not started");
        }
        return schemaRegistry.getBaseUrl();
    }

    @Override
    public void start() {}
}
