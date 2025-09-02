package de.invesdwin.context.integration.channel.sync.kafka.connect;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;
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
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaBridges;
import de.invesdwin.context.integration.channel.sync.kafka.IKafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaContainer;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaSynchronousReader;
import de.invesdwin.context.integration.channel.sync.kafka.KafkaSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.kafka.redpanda.connect.RedpandaConnectBridges;
import de.invesdwin.context.integration.channel.sync.kafka.redpanda.console.RedpandaConsoleContainer;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

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
public class KafkaKcatBridgesChannelTest extends AKafkaChannelTest {

    private static final Duration POLL_TIMEOUT = Duration.ZERO;
    @Container
    protected final RedpandaConsoleContainer redpandaConsoleContainer = newRedpandaConsoleContainer();
    @Container
    protected final IKafkaBridges kafkaBridges = newKafkaBridges();
    private final Log log = new Log(this);

    @Override
    protected IKafkaContainer<?> newKafkaContainer() {

        return new KafkaContainer();
    }

    protected IKafkaBridges newKafkaBridges() {
        return new RedpandaConnectBridges(kafkaContainer);
    }

    protected RedpandaConsoleContainer newRedpandaConsoleContainer() {
        return new RedpandaConsoleContainer(kafkaContainer);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        KafkaContainer.deleteAllTopics(kafkaContainer.getBootstrapServers());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        KafkaContainer.deleteAllTopics(kafkaContainer.getBootstrapServers());
    }

    @Test
    public void testKafkaRedpandaConnectThroughput() throws Exception {
        final String inputTopic = "throughput_input";
        final String outputTopic = "throughput_output";

        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), inputTopic);
        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), outputTopic);

        kafkaBridges.startBridge(inputTopic, outputTopic);
        runKafkaThroughputTest(inputTopic, outputTopic, kafkaContainer.getBootstrapServers());
    }

    @Test
    public void testKafkaRedpandaConnectLatency() throws Exception {
        final String requestInputTopic = "latency_request_in";
        final String requestOutputTopic = "latency_request_out";
        final String responseInputTopic = "latency_response_in";
        final String responseOutputTopic = "latency_response_out";

        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), requestInputTopic);
        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), requestOutputTopic);
        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), responseInputTopic);
        KafkaContainer.createTopic(kafkaContainer.getBootstrapServers(), responseOutputTopic);

        kafkaBridges.startBridge(requestInputTopic, requestOutputTopic);
        kafkaBridges.startBridge(responseInputTopic, responseOutputTopic);

        runKafkaLatencyTest(responseInputTopic, requestInputTopic, responseOutputTopic, requestOutputTopic,
                kafkaContainer.getBootstrapServers());
    }

    protected void runKafkaThroughputTest(final String inputTopic, final String outputTopic,
            final String bootstrapServers) throws InterruptedException {
        final boolean flush = false;

        final ISynchronousWriter<FDate> writer = newSerdeWriter(
                newKafkaSynchronousWriter(bootstrapServers, inputTopic, flush));

        final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, writer);

        final ISynchronousReader<FDate> reader = newSerdeReader(
                new KafkaSynchronousReader(bootstrapServers, outputTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });

        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, reader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    private void runKafkaLatencyTest(final String responseInputTopic, final String requestInputTopic,
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
            final String topic, final boolean flush) {
        return new KafkaSynchronousWriter(bootstrapServers, topic, flush);
    }
}
