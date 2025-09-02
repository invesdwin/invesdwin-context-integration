package de.invesdwin.context.integration.channel.sync.kafka.redpanda.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.connect.KafkaKcatBridgesChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.redpanda.RedpandaContainer;

@Testcontainers
@NotThreadSafe
public class RedpandaConnectBridgesChannelTest extends KafkaKcatBridgesChannelTest {

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new RedpandaContainer();
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