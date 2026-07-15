package de.invesdwin.context.integration.channel.sync.kafka;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.junit.jupiter.Container;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public abstract class AKafkaChannelTest extends AChannelTest {

    protected static final boolean TMPFS = false;
    protected static final boolean TRANSIENT = false;
    @Container
    protected final IKafkaContainer<?> kafkaContainer = newKafkaContainer();

    protected IKafkaContainer<?> newKafkaContainer() {
        final KafkaContainer container = new KafkaContainer();
        if (TRANSIENT) {
            container.setEnvTransient();
        }
        if (TMPFS) {
            container.setEnvTmpfs();
        }
        return container;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        KafkaContainer.deleteAllTopics(kafkaContainer.getBootstrapServers());
        log.info("Using Container %s", kafkaContainer.getClass().getName());
        log.info(" container %s", kafkaContainer.getBootstrapServers());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        KafkaContainer.deleteAllTopics(kafkaContainer.getBootstrapServers());
    }

    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic, final boolean flush) {
        //flushing on each message should theoretically send the messages slightly earlier in the latency test
        //non-flushing should be faster for the throughput test
        return new KafkaSynchronousWriter(bootstrapServers, requestTopic, flush);
    }

}
