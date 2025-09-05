package de.invesdwin.context.integration.channel.sync.kafka.confluent.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.confluent.ConfluentServerContainer;
import de.invesdwin.context.integration.channel.sync.kafka.confluent.SchemaRegistryContainer;
import de.invesdwin.context.integration.channel.sync.kafka.connect.KafkaKcatBridgesChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.redpanda.console.RedpandaConsoleContainer;

@Testcontainers
@NotThreadSafe
public class ConfluentBridgesChannelTest extends KafkaKcatBridgesChannelTest {
    @Container
    protected ConfluentConnectContainer connectContainer = newConnectContainer();
    @Container
    protected SchemaRegistryContainer schemaRegistryContainer = newSchemaRegistryContainer();

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new ConfluentServerContainer();
    }

    protected ConfluentConnectContainer newConnectContainer() {
        final ConfluentConnectContainer connectContainer = new ConfluentConnectContainer(
                (ConfluentServerContainer) kafkaContainer, null);
        redpandaConsoleContainer.setKafkaConnectContainer(connectContainer);
        return connectContainer;
    }

    private SchemaRegistryContainer newSchemaRegistryContainer() {
        final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(
                (ConfluentServerContainer) kafkaContainer, null);
        redpandaConsoleContainer.setSchemaRegistryContainer(schemaRegistryContainer);
        return schemaRegistryContainer;
    }

    @Override
    protected RedpandaConsoleContainer newRedpandaConsoleContainer() {
        return new RedpandaConsoleContainer(kafkaContainer);
    }

    //    @Override
    //    protected IKafkaBridges newKafkaBridges() {
    //        return new RedpandaConnectBridges(kafkaContainer);
    //    }

    @Override
    @Test
    public void testKafkaBridgesThroughput() throws Exception {
        super.testKafkaBridgesThroughput();
    }

    @Override
    @Test
    public void testKafkaBridgesLatency() throws Exception {
        super.testKafkaBridgesLatency();
    }
}