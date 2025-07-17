package de.invesdwin.context.integration.channel.sync.kafka;

import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

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

import com.github.dockerjava.api.command.CreateContainerCmd;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.math.decimal.scaled.ByteSizeScale;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class KafkaChannelTest extends AChannelTest {

    private static final Duration POLL_TIMEOUT = Duration.ZERO;
    private static final boolean TMPFS = false;
    private static final boolean TRANSIENT = false;

    @Container
    private static final KafkaContainer KAFKACONTAINER = newKafkaContainer();

    private static KafkaContainer newKafkaContainer() {
        final KafkaContainer container = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));
        if (TRANSIENT) {
            container.withEnv("KAFKA_LOG_RETENTION_MS", "1");
        }
        if (TMPFS) {
            container.withEnv("KAFKA_LOG_DIRS", "/dev/shm/kafka-logs");
            container.withCreateContainerCmdModifier(new Consumer<CreateContainerCmd>() {
                @Override
                public void accept(final CreateContainerCmd t) {
                    t.getHostConfig().withShmSize((long) ByteSizeScale.BYTES.convert(8, ByteSizeScale.GIGABYTES));
                }
            });
        }
        return container;
    }

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
        deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        deleteAllTopics(KAFKACONTAINER.getBootstrapServers());
    }

    @Test
    public void testKafkaLatency() throws InterruptedException {
        final String responseTopic = "testKafkaLatency_response";
        final String requestTopic = "testKafkaLatency_request";
        createTopic(KAFKACONTAINER.getBootstrapServers(), responseTopic);
        createTopic(KAFKACONTAINER.getBootstrapServers(), requestTopic);
        runKafkaLatencyTest(responseTopic, requestTopic);
    }

    @Test
    public void testKafkaThroughput() throws InterruptedException {
        final String topic = "testKafkaThroughput_chanel";
        createTopic(KAFKACONTAINER.getBootstrapServers(), topic);
        runKafkaThroughputTest(topic);
    }

    protected void runKafkaThroughputTest(final String topic) throws InterruptedException {
        final boolean flush = false;
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), topic, flush));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), topic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runKafkaLatencyTest(final String responseTopic, final String requestTopic)
            throws InterruptedException {
        final boolean flush = true;
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), responseTopic, flush));
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), requestTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newKafkaSynchronousWriter(KAFKACONTAINER.getBootstrapServers(), requestTopic, flush));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                new KafkaSynchronousReader(KAFKACONTAINER.getBootstrapServers(), responseTopic) {
                    @Override
                    protected Duration newPollTimeout() {
                        return POLL_TIMEOUT;
                    }
                });
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    protected ISynchronousWriter<IByteBufferProvider> newKafkaSynchronousWriter(final String bootstrapServers,
            final String requestTopic, final boolean flush) {
        //flushing on each message should theoretically send the messages slightly earlier in the latency test
        //non-flushing should be faster for the throughput test
        return new KafkaSynchronousWriter(bootstrapServers, requestTopic, flush);
    }

}
