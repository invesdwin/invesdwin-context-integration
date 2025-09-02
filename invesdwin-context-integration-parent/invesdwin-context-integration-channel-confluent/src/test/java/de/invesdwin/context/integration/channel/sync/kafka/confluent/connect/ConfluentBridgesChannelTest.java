package de.invesdwin.context.integration.channel.sync.kafka.confluent.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.confluent.ConfluentServerContainer;
import de.invesdwin.context.integration.channel.sync.kafka.confluent.SchemaRegistryContainer;
import de.invesdwin.context.integration.channel.sync.kafka.connect.KafkaKcatBridgesChannelTest;

@Testcontainers
@NotThreadSafe
public class ConfluentBridgesChannelTest extends KafkaKcatBridgesChannelTest {
//    private static final Network NETWORK = Network.newNetwork();
    //private static final KafkaContainer K = new KafkaContainer();
//    @Container
//    protected final SchemaRegistryContainer schemaRegistryContainer = newSchemaRegistryContainer();

    //
    protected SchemaRegistryContainer newSchemaRegistryContainer() {
        return new SchemaRegistryContainer((ConfluentServerContainer) kafkaContainer, NETWORK);
    }

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        final ConfluentServerContainer kafkaContainer = new ConfluentServerContainer();
        return kafkaContainer;
    }

    @Override
    @Test
    public void testKafkaRedpandaConnectThroughput() throws Exception {
        super.testKafkaRedpandaConnectThroughput();
    }

    @Override
    @Test
    public void testKafkaRedpandaConnectLatency() throws Exception {
        super.testKafkaRedpandaConnectLatency();
    }
}