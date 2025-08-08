package de.invesdwin.context.integration.channel.sync.kafka.confluent;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaChannelTest;

@NotThreadSafe
public class ConfluentCommercialChannelTest extends KafkaChannelTest {

    private static final boolean SCHEMA_FACTORY = true;

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {
        return new ConfluentCommercialContainer(SCHEMA_FACTORY);
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
