package de.invesdwin.context.integration.channel.kafka.redpanda;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.kafka.KafkaChannelTest;

@NotThreadSafe
public class RedpandaChannelTest extends KafkaChannelTest {

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new RedpandaContainer();
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
