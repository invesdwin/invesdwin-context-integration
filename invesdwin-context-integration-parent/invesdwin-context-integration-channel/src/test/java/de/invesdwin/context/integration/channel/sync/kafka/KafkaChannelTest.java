package de.invesdwin.context.integration.channel.sync.kafka;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class KafkaChannelLatencyTest extends AChannelTest {

    @Container
    private static final KafkaContainer KAFKACONTAINER = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.8.0"));

    @Override
    public void setUp() throws Exception {
        super.setUp();
        KafkaChannelThroughputTest.deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        KafkaChannelThroughputTest.deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Test
    public void testKafkaPerformance() throws InterruptedException {
        final String responseTopic = "testKafkaPerformance_response";
        final String requestTopic = "testKafkaPerformance_request";
        KafkaChannelThroughputTest.createTopic(KAFKACONTAINER.getBootstrapServers(), responseTopic);
        KafkaChannelThroughputTest.createTopic(KAFKACONTAINER.getBootstrapServers(), requestTopic);
        runKafkaPerformanceTest(responseTopic, requestTopic, Duration.ZERO);
    }

    @Test
    public void testBlockingKafkaPerformance() throws InterruptedException {
        final String responseTopic = "testBlockingKafkaPerformance_response";
        final String requestTopic = "testBlockingKafkaPerformance_request";
        KafkaChannelThroughputTest.createTopic(KAFKACONTAINER.getBootstrapServers(), responseTopic);
        KafkaChannelThroughputTest.createTopic(KAFKACONTAINER.getBootstrapServers(), requestTopic);
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
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), requestTopic));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), responseTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return pollTimeout;
                    }
                });
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic) {
        //flushing on each message should theoretically send the messages slightly earlier in this scenario
        return new FlushingKafkaSynchronousWriter(bootstrapServers, requestTopic);
    }

}
