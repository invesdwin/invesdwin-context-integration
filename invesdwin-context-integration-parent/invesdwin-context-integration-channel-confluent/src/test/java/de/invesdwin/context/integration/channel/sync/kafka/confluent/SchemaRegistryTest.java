package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaChannelTest;

@NotThreadSafe
public class SchemaRegistryTest extends KafkaChannelTest {

    private static final Network NETWORK = Network.newNetwork();
    @Container
    protected final SchemaRegistryContainer schemaRegistryContainer = newSchemaRegistryContainer();

    protected SchemaRegistryContainer newSchemaRegistryContainer() {
        return new SchemaRegistryContainer((ConfluentServerContainer) kafkaContainer, NETWORK);
    }

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        final ConfluentServerContainer kafkaContainer = new ConfluentServerContainer();
        kafkaContainer.withNetwork(NETWORK);
        return kafkaContainer;
    }

    @Test
    @Override
    public void testKafkaLatency() throws InterruptedException {
        super.testKafkaLatency();
    }

    @Test
    @Override
    public void testKafkaThroughput() throws InterruptedException {
        super.testKafkaThroughput();
    }
}
