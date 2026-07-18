package de.invesdwin.context.integration.channel.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.kafka.KafkaChannelTest;

@NotThreadSafe
public class ConfluentServerChannelTest extends KafkaChannelTest {

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new ConfluentServerContainer();
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
