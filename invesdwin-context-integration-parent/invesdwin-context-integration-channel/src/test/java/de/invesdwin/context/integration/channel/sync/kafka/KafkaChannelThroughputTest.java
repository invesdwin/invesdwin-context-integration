package de.invesdwin.context.integration.channel.sync.kafka;

import java.util.Properties;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.AThroughputChannelTest;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class KafkaChannelThroughputTest extends AThroughputChannelTest {

    @Container
    private static final KafkaContainer KAFKACONTAINER = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"));

    public static void createTopic(final String bootstrapServers, final String topic) {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(config)) {
            final NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteAllTopics(final String bootstrapServers) {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(config)) {
            final ListTopicsResult listTopics = adminClient.listTopics();
            final Set<String> names = listTopics.names().get();
            adminClient.deleteTopics(names).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        KafkaChannelThroughputTest.deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Test
    public void testKafkaPerformance() throws InterruptedException {
        final String topic = "testKafkaPerformance_chanel";
        createTopic(KAFKACONTAINER.getBootstrapServers(), topic);
        runKafkaPerformanceTest(topic, Duration.ZERO);
    }

    @Test
    public void testBlockingKafkaPerformance() throws InterruptedException {
        final String topic = "testBlockingKafkaPerformance_topic";
        createTopic(KAFKACONTAINER.getBootstrapServers(), topic);
        runKafkaPerformanceTest(topic, Duration.ONE_MILLISECOND);
    }

    protected void runKafkaPerformanceTest(final String topic, final Duration pollTimeout) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), topic));
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKafkaPerformanceTest", 1);
        executor.execute(new ThroughputSenderTask(channelWriter));
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), topic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return pollTimeout;
                    }
                });
        new ThroughputReceiverTask(channelReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic) {
        return new KafkaSynchronousWriter(bootstrapServers, requestTopic);
    }

}
