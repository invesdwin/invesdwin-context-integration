package de.invesdwin.context.integration.channel.sync.kafka.confluent.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.confluent.ConfluentCommunityContainer;
import de.invesdwin.context.integration.channel.sync.kafka.connect.KafkaKcatBridgesChannelTest;

// TODO: extend KafkaConnectChannelTest for Redpanda and replace KafkaContainer with RedpandaContainer and
// use RedpandaConnectBridges also directly in KafkaConnectChannelTest (until we find a replacement)DONE
// TODO: create a ConfluentConnectChannelTest that integrates confluents kafka connect and schema registry with redpanda
// console; for now using RedpandaConnectBridges (extending the KafkaConnectChannelTest similar to
// RedpandaConnectChannelTest)
// TODO: create ConfluentConnectBridges (ideally expending KafkaConnectBridges and just replacing the image name) and
// ConfluentConnectChannelTest (might have to do the host/gateway ip address and bootstrapServers without PLAINTEXT://
// prefix workarounds as well) (postponed until we find kafka connect bridges similar to redpanda connect)
// TODO: create a KafkaConnectBridges and KafkaConnectChannelTest (postponed until we find kafka connect bridges similar
// to redpanda connect)

@Testcontainers
@NotThreadSafe
public class ConfluentConnectChannelTest extends KafkaKcatBridgesChannelTest {

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new ConfluentCommunityContainer();
    }

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