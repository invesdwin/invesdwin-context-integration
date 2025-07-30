package de.invesdwin.context.integration.channel.sync.kafka;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class KafkaChannelTest extends AKafkaChannelTest {

    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    @Test
    public void testKafkaLatency() throws InterruptedException {
        final String responseTopic = "testKafkaLatency_response";
        final String requestTopic = "testKafkaLatency_request";
        final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        KafkaContainerForNifi.createTopic(bootstrapServers, responseTopic);
        KafkaContainerForNifi.createTopic(bootstrapServers, requestTopic);
        runKafkaLatencyTest(bootstrapServers, responseTopic, requestTopic);
    }

    @Test
    public void testKafkaThroughput() throws InterruptedException {
        final String topic = "testKafkaThroughput_channel";
        final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        KafkaContainerForNifi.createTopic(bootstrapServers, topic);
        runKafkaThroughputTest(bootstrapServers, topic);
    }

    protected void runKafkaThroughputTest(final String bootstrapServers, final String topic)
            throws InterruptedException {
        final boolean flush = false;
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, topic, flush));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, topic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runKafkaLatencyTest(final String bootstrapServers, final String responseTopic,
            final String requestTopic) throws InterruptedException {
        final boolean flush = true;
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, responseTopic, flush));
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, requestTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, requestTopic, flush));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, responseTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

}
