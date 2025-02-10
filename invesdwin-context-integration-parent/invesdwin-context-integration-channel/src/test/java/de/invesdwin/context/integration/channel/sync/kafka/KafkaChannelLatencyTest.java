package de.invesdwin.context.integration.channel.sync.kafka;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.ALatencyChannelTest;
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
public class KafkaChannelLatencyTest extends ALatencyChannelTest {

    @Container
    private static final KafkaContainer KAFKACONTAINER = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"));

    private void createTopic(final String topic) {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKACONTAINER.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(config)) {
            final NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testKafkaPerformance() throws InterruptedException {
        final String responseTopic = "testKafkaPerformance_response";
        final String requestTopic = "testKafkaPerformance_request";
        createTopic(responseTopic);
        createTopic(requestTopic);
        runKafkaPerformanceTest(responseTopic, requestTopic, Duration.ZERO);
    }

    @Test
    public void testBlockingKafkaPerformance() throws InterruptedException {
        final String responseTopic = "testBlockingKafkaPerformance_response";
        final String requestTopic = "testBlockingKafkaPerformance_request";
        createTopic(responseTopic);
        createTopic(requestTopic);
        runKafkaPerformanceTest(responseTopic, requestTopic, Duration.ONE_MILLISECOND);
    }

    protected void runKafkaPerformanceTest(final String responseTopic, final String requestTopic,
            final Duration pollTimeout) throws InterruptedException {
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), responseTopic));
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), requestTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return pollTimeout;
                    }
                });
        final WrappedExecutorService executor = Executors.newFixedThreadPool("runKafkaPerformanceTest", 1);
        executor.execute(new LatencyServerTask(requestReader, responseWriter));
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), requestTopic));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), responseTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return pollTimeout;
                    }
                });
        new LatencyClientTask(requestWriter, responseReader).run();
        executor.shutdown();
        executor.awaitTermination();
    }

    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic) {
        //flushing on each message should theoretically send the messages slightly earlier in this scenario
        return new FlushingKafkaSynchronousWriter(bootstrapServers, requestTopic);
    }

}
