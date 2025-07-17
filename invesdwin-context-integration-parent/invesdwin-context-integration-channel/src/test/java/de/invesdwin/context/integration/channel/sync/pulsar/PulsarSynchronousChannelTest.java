package de.invesdwin.context.integration.channel.sync.pulsar;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class PulsarSynchronousChannelTest extends AChannelTest {
    @Container
    private static final PulsarContainer PULSARCONTAINER = newPulsarContainer();
    private static final boolean BLOCKING = false;
    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    private static PulsarContainer newPulsarContainer() {
        final PulsarContainer container = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.5"));
        return container;
    }

    public static void createTopic(final PulsarAdmin admin, final String topic) {
        try {
            admin.topics().createNonPartitionedTopic(topic);
        } catch (final PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteAllTopics(final PulsarAdmin admin) throws PulsarAdminException {
        final List<String> tenants = admin.tenants().getTenants();
        for (final String tenant : tenants) {
            final List<String> namespaces = admin.namespaces().getNamespaces(tenant);
            for (final String namespace : namespaces) {
                if ("public/functions".equals(namespace)) {
                    continue;
                }
                final List<String> topics = admin.topics().getList(namespace);
                for (final String topic : topics) {
                    admin.topics().unload(topic);
                    admin.topics().delete(topic);
                }
            }
        }
    }

    private static PulsarAdmin newAdmin() {
        try {
            return PulsarAdmin.builder().serviceHttpUrl(newServiceHttpUrl()).build();
        } catch (final PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static String newServiceHttpUrl() {
        //return "http://localhost:8080";
        return PULSARCONTAINER.getHttpServiceUrl();
    }

    private String newPulsarBrokerUrl() {
        //        return "pulsar://localhost:6650";
        return PULSARCONTAINER.getPulsarBrokerUrl();
    }

    @Override
    public void setUp() throws Exception {
        //using a new admin as putting an admin as input in this method would overwrite the super.setup()
        super.setUp();
        try (PulsarAdmin admin = newAdmin()) {
            deleteAllTopics(admin);
        }
    }

    @Override
    public void tearDown() throws Exception {
        //using a new admin as putting an admin as input in this method would overwrite the super.teardown()
        super.tearDown();
        try (PulsarAdmin admin = newAdmin()) {
            deleteAllTopics(admin);
        }
    }

    @Test
    public void testPulsarLatencyConsumer() throws InterruptedException {
        final String responseTopic = "testPulsarLatencyConsumer_response";
        final String requestTopic = "testPulsarLatencyConsumer_request";
        try (PulsarAdmin admin = newAdmin()) {
            createTopic(admin, responseTopic);
            createTopic(admin, requestTopic);
        }
        runPulsarLatencyTest(responseTopic, requestTopic, false);
    }

    @Test
    public void testPulsarLatencyReader() throws InterruptedException {
        final String responseTopic = "testPulsarLatencyReader_response";
        final String requestTopic = "testPulsarLatencyReader_request";
        try (PulsarAdmin admin = newAdmin()) {
            createTopic(admin, responseTopic);
            createTopic(admin, requestTopic);
        }
        runPulsarLatencyTest(responseTopic, requestTopic, true);
    }

    @Test
    public void testPulsarThroughputConsumer() throws InterruptedException {
        final String topic = "testPulsarThroughputConsumer_channel";
        try (PulsarAdmin admin = newAdmin()) {
            createTopic(admin, topic);
        }
        runPulsarThroughputTest(topic, false);
    }

    @Test
    public void testPulsarThroughputReader() throws InterruptedException {
        final String topic = "testPulsarThroughputReader_channel";
        try (PulsarAdmin admin = newAdmin()) {
            createTopic(admin, topic);
        }
        runPulsarThroughputTest(topic, true);
    }

    protected void runPulsarThroughputTest(final String topic, final boolean useReader) throws InterruptedException {
        final boolean flush = true;
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newPulsarProducerSynchronousWriter(new PulsarSynchronousChannel(newPulsarBrokerUrl()), topic, flush));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                newPulsarSynchronousReader(new PulsarSynchronousChannel(newPulsarBrokerUrl()), topic, useReader));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runPulsarLatencyTest(final String responseTopic, final String requestTopic, final boolean useReader)
            throws InterruptedException {
        final boolean flush = true;
        final ISynchronousReader<FDate> requestReader = newSerdeReader(newPulsarSynchronousReader(
                new PulsarSynchronousChannel(newPulsarBrokerUrl()), requestTopic, useReader));
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(newPulsarProducerSynchronousWriter(
                new PulsarSynchronousChannel(newPulsarBrokerUrl()), responseTopic, flush));

        final ISynchronousReader<FDate> responseReader = newSerdeReader(newPulsarSynchronousReader(
                new PulsarSynchronousChannel(newPulsarBrokerUrl()), responseTopic, useReader));

        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(newPulsarProducerSynchronousWriter(
                new PulsarSynchronousChannel(newPulsarBrokerUrl()), requestTopic, flush));

        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);
        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    private ISynchronousReader<IByteBufferProvider> newPulsarSynchronousReader(final PulsarSynchronousChannel channel,
            final String topic, final boolean useReader) {
        if (useReader) {
            return new PulsarReaderSynchronousReader(channel, topic) {
                @Override
                protected Duration newPollTimeout() {
                    return POLL_TIMEOUT;
                }
            };
        } else {
            return new PulsarConsumerSynchronousReader(channel, topic) {
                @Override
                protected Duration newPollTimeout() {
                    return POLL_TIMEOUT;
                }
            };
        }
    }

    protected ISynchronousWriter<IByteBufferProvider> newPulsarProducerSynchronousWriter(
            final PulsarSynchronousChannel channel, final String topic, final boolean flush) {
        //flushing on each message should theoretically send the messages slightly earlier in the latency test
        //non-flushing should be faster for the throughput test
        return new PulsarProducerSynchronousWriter(channel, topic, flush, BLOCKING);
    }
}
