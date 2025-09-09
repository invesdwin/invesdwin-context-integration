package de.invesdwin.context.integration.channel.sync.kafka.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.kafka.AKafkaChannelTest;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaSynchronousReader;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaSynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class KafkaNifiChannelTest extends AKafkaChannelTest {

    @Container
    protected static final NifiContainer NIFI_CONTAINER = newNifiContainer();
    private static final boolean STATELESS = false;
    private static final boolean EXACTLY_ONCE = false;
    private static final int PARALLEL_TASKS_COUNT = 1;
    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    protected static NifiContainer newNifiContainer() {
        return new NifiContainer();
    }

    /*
     * exactly once requires DeliveryGuarantee=GuaranteeReplicatedDelivery which is achieved via acks=all in the json
     * file. exactly once semantics with kafka requires stateless process group in nifi to function correctly
     */
    public static String newFlowFileForKafka(final Resource flowResource, final String bootstrapServers) {
        try (InputStream in = flowResource.getInputStream()) {
            final String jsonStr = IOUtils.toString(in, StandardCharsets.UTF_8);
            return jsonStr.replace("{KAFKA_INTERNAL_BOOTSTRAP_SERVERS}", bootstrapServers)
                    .replace("{STATELESS_SCHEDULED_STATE}", STATELESS ? "DISABLED" : "ENABLED")
                    .replace("{STATELESS_EXECUTION_ENGINE}", STATELESS ? "STATELESS" : "INHERITED")
                    .replace("{EXACTLY_ONCE_ACKS}", EXACTLY_ONCE ? "all" : "0")
                    .replace("{EXACTLY_ONCE_TRANSACTIONS_ENABLED}", EXACTLY_ONCE ? "true" : "false")
                    .replace("{EXACTLY_ONCE_COMMIT_OFFSETS}", EXACTLY_ONCE ? "false" : "true")
                    .replace("{PARALLEL_TASKS_COUNT}", String.valueOf(PARALLEL_TASKS_COUNT));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testKafkaNifiLatency() throws InterruptedException {
        final String bootstrapServers = kafkaContainer.getBootstrapServers();
        try {
            final String flowJson = newFlowFileForKafka(
                    new ClassPathResource("NiFi_Flow_Latency.json", KafkaNifiChannelTest.class), bootstrapServers);
            NIFI_CONTAINER.setupFlow(flowJson);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final String responseInputTopic = "testKafkaNifiLatency_responseInput";
        final String requestInputTopic = "testKafkaNifiLatency_requestInput";
        final String responseOutputTopic = "testKafkaNifiLatency_responseOutput";
        final String requestOutputTopic = "testKafkaNifiLatency_requestOutput";
        runKafkaNifiLatencyTest(responseInputTopic, requestInputTopic, responseOutputTopic, requestOutputTopic,
                bootstrapServers);
    }

    @Test
    public void testKafkaNifiThroughput() throws InterruptedException {
        final String bootstrapServers = kafkaContainer.getBootstrapServers();
        try {
            final String flowJson = newFlowFileForKafka(
                    new ClassPathResource("NiFi_Flow_Throughput.json", KafkaNifiChannelTest.class), bootstrapServers);
            NIFI_CONTAINER.setupFlow(flowJson);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final String inputTopic = "testKafkaNifiThroughput_inputChannel";
        final String outputTopic = "testKafkaNifiThroughput_outputChannel";
        runKafkaNifiThroughputTest(inputTopic, outputTopic, bootstrapServers);
    }

    protected void runKafkaNifiThroughputTest(final String inputTopic, final String outputTopic,
            final String bootstrapServers) throws InterruptedException {
        final boolean flush = false;
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, inputTopic, flush));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, outputTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runKafkaNifiLatencyTest(final String responseInputTopic, final String requestInputTopic,
            final String responseOutputTopic, final String requestOutputTopic, final String bootstrapServers)
            throws InterruptedException {
        final boolean flush = true;

        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, responseInputTopic, flush));
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, requestOutputTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, requestInputTopic, flush));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, responseOutputTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic, final boolean flush) {
        //flushing on each message should theoretically send the messages slightly earlier in the latency test
        //non-flushing should be faster for the throughput test
        return new KafkaSynchronousWriter(bootstrapServers, requestTopic, flush);
    }

}