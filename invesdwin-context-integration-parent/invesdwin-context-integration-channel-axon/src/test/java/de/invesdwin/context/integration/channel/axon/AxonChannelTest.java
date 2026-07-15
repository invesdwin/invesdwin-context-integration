package de.invesdwin.context.integration.channel.axon;

import javax.annotation.concurrent.NotThreadSafe;

import org.axonframework.test.server.AxonServerContainer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.integration.channel.AChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyClientTask;
import de.invesdwin.context.integration.channel.LatencyChannelTest.LatencyServerTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputReceiverTask;
import de.invesdwin.context.integration.channel.ThroughputChannelTest.ThroughputSenderTask;
import de.invesdwin.context.integration.channel.axon.AxonSynchronousChannel;
import de.invesdwin.context.integration.channel.axon.AxonSynchronousReader;
import de.invesdwin.context.integration.channel.axon.AxonSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.duration.Duration;

@Testcontainers
@NotThreadSafe
public class AxonChannelTest extends AChannelTest {
    @Container
    private static final AxonServerContainer AXON_CONTAINER = newAxonServerContainer();
    private static final Duration POLL_TIMEOUT = Duration.ZERO;

    private static AxonServerContainer newAxonServerContainer() {
        return new AxonServerContainer().withAxonServerName("axonserver-test").withAxonServerHostname("localhost");
    }

    @Test
    public void testAxonLatency() throws InterruptedException {
        final String pulsarBrokerUrl = newAxonUrl();
        final String responseTopic = "testAxonLatency_response";
        final String requestTopic = "testAxonLatency_request";
        runAxonLatencyTest(pulsarBrokerUrl, responseTopic, requestTopic, true);
    }

    private String newAxonUrl() {
        return AXON_CONTAINER.getHost() + ":" + AXON_CONTAINER.getGrpcPort();
    }

    @Test
    public void testAxonThroughput() throws InterruptedException {
        final String pulsarBrokerUrl = newAxonUrl();
        final String topic = "testAxonThroughput_channel";
        runAxonThroughputTest(pulsarBrokerUrl, topic);
    }

    protected void runAxonThroughputTest(final String pulsarBrokerUrl, final String topic) throws InterruptedException {
        final ISynchronousWriter<FDate> channelWriter = newSerdeWriter(
                newAxonSynchronousWriter(new AxonSynchronousChannel(pulsarBrokerUrl), topic));
        final ThroughputSenderTask senderTask = new ThroughputSenderTask(this, channelWriter);
        final ISynchronousReader<FDate> channelReader = newSerdeReader(
                newAxonSynchronousReader(new AxonSynchronousChannel(pulsarBrokerUrl), topic));
        final ThroughputReceiverTask receiverTask = new ThroughputReceiverTask(this, channelReader);
        new ThroughputChannelTest(this).runThroughputTest(senderTask, receiverTask);
    }

    protected void runAxonLatencyTest(final String serverUrl, final String responseTopic, final String requestTopic,
            final boolean useReader) throws InterruptedException {
        final AxonSynchronousChannel serverChannel = new AxonSynchronousChannel(serverUrl);
        final ISynchronousReader<FDate> requestReader = newSerdeReader(
                newAxonSynchronousReader(serverChannel, requestTopic));
        final ISynchronousWriter<FDate> responseWriter = newSerdeWriter(
                newAxonSynchronousWriter(serverChannel, responseTopic));
        final LatencyServerTask serverTask = new LatencyServerTask(this, requestReader, responseWriter);

        final AxonSynchronousChannel clientChannel = new AxonSynchronousChannel(serverUrl);
        final ISynchronousWriter<FDate> requestWriter = newSerdeWriter(
                newAxonSynchronousWriter(clientChannel, requestTopic));
        final ISynchronousReader<FDate> responseReader = newSerdeReader(
                newAxonSynchronousReader(clientChannel, responseTopic));
        final LatencyClientTask clientTask = new LatencyClientTask(this, requestWriter, responseReader);

        new LatencyChannelTest(this).runLatencyTest(serverTask, clientTask);
    }

    private ISynchronousReader<IByteBufferProvider> newAxonSynchronousReader(final AxonSynchronousChannel channel,
            final String topic) {
        return new AxonSynchronousReader(channel, topic) {
            @Override
            protected Duration newPollTimeout() {
                return POLL_TIMEOUT;
            }
        };
    }

    protected ISynchronousWriter<IByteBufferProvider> newAxonSynchronousWriter(final AxonSynchronousChannel channel,
            final String topic) {
        return new AxonSynchronousWriter(channel, topic);
    }
}
